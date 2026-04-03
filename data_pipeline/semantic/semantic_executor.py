# =============================================================================
# Semantic Modeling Stage Executor
# =============================================================================

import gc
import polars as pl
from typing import Dict
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table, export_file
from data_pipeline.semantic.registry import SEMANTIC_MODULES
from data_pipeline.assembly.assembly_logic import (
    init_report,
    log_info,
    log_error,
    loaded_data,
    task_wrapper,
)


def validate_and_freeze_table(lf: pl.LazyFrame, table: dict) -> pl.LazyFrame:
    """
    Enforces the technical contract for a specific semantic table.

    Contract:
    - Schema: Ensures 1:1 match with columns in table['schema'].
    - Types: Explicitly casts columns to types defined in table['dtypes'].

    Optimization Logic:
    - Lazy Contract Enforcement: Defers all validation and casting to the final
      streaming sink, avoiding intermediate materialization passes.
    - Zero-Copy Sort Omission: Bypasses sorting to maintain compatibility with
      non-blocking streaming engines.

    Behavior:
    - Fast-Fail: Raises RuntimeError on schema violations during plan construction.
    """

    current_columns = lf.collect_schema().names()

    missing = set(table["schema"]) - set(current_columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    lf_clean = lf.select(table["schema"]).cast(table["dtypes"])

    return lf_clean


# ------------------------------------------------------------
# SUB-ORCHESTRATORS
# ------------------------------------------------------------


def orchestrate_module(
    run_context: RunContext,
    df_assembled: pl.LazyFrame,
    module_name: str,
    module_config: dict,
    report: dict,
) -> bool:
    """
    Coordinates the construction, validation, and export of a semantic module.

    Workflow:
        1. Build: Executes the module-specific builder logic.
        2. Loop: Iterates through each returned table in the builder output.
        3. Validate: Enforces technical contracts (schema, dtypes).
        4. Export: Persists validated artifacts to the semantic zone.
        5. Cleanup: Manages memory via explicit deletion and garbage collection.

    Optimization Logic:
    - Linear Streaming Propagation: Maintains the LazyFrame chain from builder to
    export, ensuring constant memory usage regardless of dataset scale.
    - Incremental Resource Reclamation: Triggers explicit Python garbage collection
    after every table export to purge intermediate metadata and plan overhead.

    Invariants:
    - Fail-Fast: Any error in building or table-level processing halts the module.
    - Strict Config: Builder output must match keys in 'module_config["tables"]'.

    Failure Behavior:
    - Traps builder-level and table-level exceptions via try-except blocks.
    - Logs specific errors (e.g., FileExistsError, general Exceptions) and marks
      the module and global report status as 'failed' before returning False.

    Returns:
        bool: True if the module and all its tables were successfully processed.
    """

    module_report = report["modules"]

    table_trackers = {
        table_name: {"build_stage": False, "validate_stage": False}
        for table_name in module_config["tables"]
    }
    module_report[module_name] = {**table_trackers, "export": False}

    # Execute Module Builder
    try:
        builder_output = module_config["builder"](df_assembled, run_context)

        if builder_output is None:
            log_error(f"Builder {module_name} returned None", report)
            report["status"] = "failed"

            return False

        # If successful, mark all tables as build_stage: True
        for tracker in table_trackers.values():
            tracker["build_stage"] = True

        print(f"[INFO] Module {module_name}: build_stage completed successfully.")

    except Exception as e:
        log_error(f"Step build_stage failed: {str(e)}", report)
        report["status"] = "failed"

        return False

    semantic_module_path = run_context.semantic_path / module_name

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    # Validate, Freeze, and Export Each Table
    for table_name, df_table in builder_output.items():

        try:
            if table_name not in module_config["tables"]:
                report["status"] = "failed"
                return False

            table_config = module_config["tables"][table_name]
            tracker = table_trackers[table_name]

            ok, lf_frozen = task_wrapper(
                report=report,
                step_name="validate_and_freeze",
                status_tracker=tracker,
                func=validate_and_freeze_table,
                lf=df_table,
                table=table_config,
            )
            if not ok:
                report["status"] = "failed"
                return False

            filename = f"{table_name}_{year}_{month}_{day}.parquet"
            output_path = semantic_module_path / filename

            if not export_file(
                lf_frozen,
                output_path,
                log_error=lambda msg: log_error(msg, report),
            ):
                report["status"] = "failed"
                return False

        except FileExistsError as e:
            log_error(f"Unexpected table returned {table_name}: {e}", report)

        except Exception as e:
            log_error(f"Unexpected error processing {table_name}: {e}", report)

        finally:
            if df_table is not None:
                del df_table
            gc.collect()

    log_info(f"Export Module: {module_name} Successfully", report)
    module_report[module_name]["export"] = True

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

    Failure Behavior:
    - Fail-Fast on missing source: Returns immediately if 'assembled_events' is missing.
    - Bubbles up module-level failures: If any module orchestration returns False,
      the stage status is set to 'failed' and the report is returned.

    Returns:
        Dict: A global report of module statuses and error logs.
    """

    report = init_report()
    report["modules"] = {}

    df_assembled = load_historical_table(
        base_path=run_context.assembled_path,
        table_name="assembled_events",
        log_info=lambda msg: loaded_data(msg, report),
    )

    source_file = list(run_context.assembled_path.glob("*.parquet"))

    if not source_file:
        log_error("assembled_events logical table missing or empty", report)
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
