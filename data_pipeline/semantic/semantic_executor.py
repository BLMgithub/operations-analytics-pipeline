# =============================================================================
# Semantic Modeling Stage Executor
# =============================================================================

import gc

# import pandas as pd
import polars as pl
from typing import Dict, Any
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_single_delta, export_file
from data_pipeline.semantic.semantic_logic import init_report, log_error, log_info
from data_pipeline.semantic.registry import SEMANTIC_MODULES


def task_wrapper(
    step_name: str,
    report: dict,
    func,
    *args,
    **kwargs,
) -> tuple[bool, Any]:
    """
    Unified task runner that handles logging, reporting, and execution.

    Inputs:
    - step_name: The lookup key in the report['steps'] dictionary.
    - report: The shared state dictionary initialized by 'init_stage_report'.
    - func: The logic/transformation function to be executed.
    - *args: Positional arguments passed directly to 'func'.

    Outputs:
    - Returns a tuple of (Success Boolean, Result Data).
    - Result Data is 'None' if the task fails or returns no data.

    Invariants:
    - Guaranteed return of (bool, Result|None).
    - Ensures report[step_name] initialization and status updates.

    Failures:
    - Traps all exceptions; returns False/None and logs the error to telemetry.
    - Returns False if the underlying function returns None (logical failure).
    """

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


def validate_and_freeze_table(df: pl.DataFrame, table: dict) -> pl.DataFrame:
    """
    Enforces the technical contract for a specific semantic table.

    Contract:
    - Grain: Validates uniqueness of columns defined in table['grain'].
    - Schema: Ensures 1:1 match with columns in table['schema'].
    - Types: Explicitly casts columns to types defined in table['dtypes'].

    Behavior:
    - Deterministic Output: Performs a stable sort based on the grain.
    - Fast-Fail: Raises RuntimeError on grain or schema violations.
    """

    # Pandas Implementation
    # if df.is_duplicated(table["grain"]).any():
    # df_clean = df[table["schema"]].astype(table["dtypes"])
    # df_clean = df_clean.sort_values(table["grain"]).reset_index(drop=True)

    # Validate duplicates

    # Polars Implementation
    if df.select(table["grain"]).is_duplicated().any():
        raise RuntimeError(f"Duplicates found in grain: {table['grain']}")

    # Validate required columns
    missing = set(table["schema"]) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    # Enforce dtypes & subset columns
    df_clean = df.select(table["schema"]).cast(table["dtypes"])

    # Deterministic sort
    df_clean = df_clean.sort(table["grain"])

    return df_clean


# ------------------------------------------------------------
# SUB-ORCHESTRATORS
# ------------------------------------------------------------


def orchestrate_module(
    run_context: RunContext,
    df_assembled: pl.DataFrame,
    module_name: str,
    module_config: dict,
    report: dict,
) -> bool:
    """
    Coordinates the construction, validation, and export of a semantic module.

    Workflow:
        1. Build: Executes the module-specific builder logic.
        2. Loop: Iterates through each returned table in the builder output.
        3. Validate: Enforces technical contracts (grain, schema, dtypes).
        4. Export: Persists validated artifacts to the semantic zone.
        5. Cleanup: Manages memory via explicit deletion and garbage collection.

    Invariants:
    - Fail-Fast: Any error in building or table-level processing halts the module.
    - Strict Config: Builder output must match keys in 'module_config["tables"]'.

    Returns:
        bool: True if the module and all its tables were successfully processed.
    """

    module_report = init_report()
    report["modules"][module_name] = module_report

    # Execute Module Builder
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

        table = module_config["tables"][table_name]

        # Apply Freeze Contract
        ok, df_frozen = task_wrapper(
            step_name="validate_and_freeze",
            report=table_report,
            func=validate_and_freeze_table,
            df=df_table,
            table=table,
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
    Main entry point for the Gold-to-Semantic stage.

    Workflow:
        1. Source Verification: Loads 'assembled_events' and halts if empty/missing.
        2. Registry Execution: Iterates through 'SEMANTIC_MODULES'.
        3. Orchestration: Triggers builder logic followed by contract enforcement.
        4. Cleanup: Purges memory after each module export.

    Guarantees:
    - Atomicity: Module failures are trapped but mark the entire stage as 'failed'.
    - Lineage: Uses 'run_id' for deterministic output partitioning.

    Returns:
        Dict: A global report of module statuses and error logs.
    """

    report = {
        "status": "success",
        "steps": {"load_tables": init_report()},
        "modules": {},
    }

    load_report = report["steps"]["load_tables"]

    df_assembled, _ = load_single_delta(
        engine="Polars",
        base_path=run_context.assembled_path,
        table_name="assembled_events",
        log_info=lambda msg: log_info(msg, load_report),
    )

    if df_assembled is None or df_assembled.is_empty():
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
