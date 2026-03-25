# =============================================================================
# Semantic Modeling Stage Executor
# =============================================================================

import gc
import pandas as pd
from typing import Dict, Any
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table, export_file
from data_pipeline.semantic.semantic_logic import init_report, log_error, log_info
from data_pipeline.semantic.registy import SEMANTIC_MODULES


def task_wrapper(
    step_name: str,
    report: dict,
    func,
    *args,
    **kwargs,
) -> tuple[bool, Any]:
    """Unified task runner that handles logging, reporting, and execution."""

    if step_name not in report:
        report[step_name] = init_report()

    step_report = report[step_name]

    try:
        result = func(*args, **kwargs)
        if result is None:
            step_report["status"] = "failed"

            return False, None

        step_report["status"] = "success"

        return True, result

    except Exception as e:
        log_error(str(e), step_report)
        step_report["status"] = "failed"

        return False, None


def validate_and_freeze_table(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """Pure logic function for contract enforcement."""

    # Validate duplicates
    if df.duplicated(meta["grain"]).any():
        raise RuntimeError(f"Duplicates found in grain: {meta['grain']}")

    # Validate required columns
    missing = set(meta["schema"]) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    # Enforce dtypes & subset columns
    df_clean = df[meta["schema"]].astype(meta["dtypes"])

    # Deterministic sort
    df_clean = df_clean.sort_values(meta["grain"]).reset_index(drop=True)

    return df_clean


# ------------------------------------------------------------
# SUB-ORCHESTRATORS
# ------------------------------------------------------------


def orchestrate_module(
    run_context: RunContext,
    df_assembled: pd.DataFrame,
    module_name: str,
    module_config: dict,
    report: dict,
) -> bool:
    """Processes a single semantic module (e.g., seller_semantic)."""

    module_report = init_report()
    report["modules"][module_name] = module_report

    # 1. Execute Module Builder
    ok, builder_output = task_wrapper(
        "build_stage",
        module_report,
        module_config["builder"],
        df_assembled,
        run_context,
    )
    if not ok:
        return False

    semantic_module_path = run_context.semantic_path / module_name

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    # Validate, Freeze, and Export Each Table
    for table_name, df_table in builder_output.items():
        table_report = init_report()
        module_report[table_name] = table_report

        if table_name not in module_config["tables"]:
            log_error(f"Unexpected table returned: {table_name}", module_report)
            return False

        meta = module_config["tables"][table_name]

        # Apply Freeze Contract
        ok, df_frozen = task_wrapper(
            "validate_and_freeze",
            table_report,
            validate_and_freeze_table,
            df_table,
            meta,
        )
        if not ok:
            return False

        # Export Artifact
        filename = f"{table_name}_{year}_{month}_{day}.parquet"
        output_path = semantic_module_path / filename

        if not export_file(df_frozen, output_path):
            log_error("Export failed", table_report)
            return False

        log_info(f"Export success: {filename} ({len(df_frozen)} rows)", table_report)

        del df_table, df_frozen
        gc.collect()

    return True


def build_semantic_layer(run_context: RunContext) -> Dict:
    """
    Builds semantic modules from the assembled event layer.
    """
    report = {
        "status": "success",
        "steps": {"load_tables": init_report()},
        "modules": {},
    }

    load_report = report["steps"]["load_tables"]

    df_assembled = load_historical_table(
        run_context.assembled_path,
        "assembled_events",
        log_info=lambda msg: log_info(msg, load_report),
    )

    if df_assembled is None or df_assembled.empty:
        log_error("assembled_events logical table missing or empty", load_report)
        report["status"] = "failed"
        return report

    for module_name, module_config in SEMANTIC_MODULES.items():
        if not orchestrate_module(
            run_context,
            df_assembled,
            module_name,
            module_config,
            report,
        ):
            report["status"] = "failed"
            return report

    del df_assembled
    gc.collect()

    return report
