# =============================================================================
# Semantic Modeling Stage Executor
# =============================================================================

import polars as pl
from typing import Dict
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_assembled_data, export_file
from data_pipeline.semantic.registry import SEMANTIC_MODULES
from data_pipeline.assembly.assembly_logic import (
    init_report,
    log_info,
    log_error,
    loaded_data,
    task_wrapper,
)
from data_pipeline.assembly.assembly_executor import force_gc


def validate_and_freeze_table(lf: pl.LazyFrame, table: dict) -> pl.LazyFrame:
    """
    Enforces the technical contract for a specific semantic table.

    Contract:
    - Schema: Ensures 1:1 match with columns in table['schema'].
    - Types: Explicitly casts columns to types defined in table['dtypes'].

    Optimization Logic:
    - Lazy Contract Enforcement: Defers all validation and casting to the final streaming sink, avoiding intermediate materialization passes.
    - Zero-Copy Sort Omission: Bypasses sorting to maintain compatibility with non-blocking streaming engines.

    Invariants:
    - Schema Integrity: Output exactly matches the registry specification.

    Outputs:
    - Validated LazyFrame ready for export.

    Failures:
    - [Structural] Raises RuntimeError on schema violations during plan construction.
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
    1. Delegate: Executes module-specific builder logic with assembled data.
    2. Validate: Enforces technical contracts (schema, dtypes) for each table.
    3. Promote: Persists validated artifacts to the semantic zone.
    4. Purge: Manages memory via explicit deletion and garbage collection.

    Optimization Logic:
    - Linear Streaming Propagation: Maintains the LazyFrame chain from builder to export, ensuring constant memory usage regardless of dataset scale.
    - Incremental Resource Reclamation: Triggers explicit Python garbage collection after every table export to purge intermediate metadata and plan overhead.

    Operational Guarantees:
    - Fail-Fast: Any error in building or table-level processing halts the module immediately.
    - Atomic Module Status: Marks the global report as 'failed' upon any internal exception.

    Side Effects:
    - Persists multiple Parquet files to the semantic directory.
    - Mutates the 'report' dictionary with step-level statuses.

    Failure Behavior:
    - Traps builder-level and table-level exceptions; logs errors and returns False.

    Returns:
        bool: True if the module and all its tables were successfully processed.
    """

    module_report = report["modules"]

    table_trackers = {
        table_name: {
            "build_stage": False,
            "validate_stage": False,
        }
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
        log_error(f"Step build_stage failed: {e}", report)
        report["status"] = "failed"

        return False

    semantic_module_path = run_context.semantic_path / module_name

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    # Validate, Freeze, and Export Each Table
    table_names = list(builder_output.keys())
    for table_name in table_names:
        lf_frozen = None
        df_table = builder_output.pop(table_name)

        try:
            if table_name not in module_config["tables"]:
                report["status"] = "failed"
                return False

            table_config = module_config["tables"][table_name]
            tracker = table_trackers[table_name]

            ok, lf_frozen = task_wrapper(
                report=report,
                step_name="validate_stage",
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
            report["status"] = "failed"
            return False

        except Exception as e:
            log_error(f"Unexpected error processing {table_name}: {e}", report)
            report["status"] = "failed"
            return False

        finally:
            if "lf_frozen" in locals():
                del lf_frozen
            if "df_table" in locals():
                del df_table
            force_gc()

    if "builder_output" in locals():
        del builder_output
    force_gc()

    log_info(f"Export Module: {module_name} Successfully", report)
    module_report[module_name]["export"] = True

    return True


def build_semantic_layer(run_context: RunContext) -> Dict:
    """
    Main entry point for the Gold-to-Semantic stage.

    Workflow:
    1. Hydrate: Loads 'assembled_events' from the assembly zone.
    2. Delegate: Iterates through SEMANTIC_MODULES registry and triggers orchestration.
    3. Purge: Clears the assembled event frame and triggers garbage collection.

    Operational Guarantees:
    - Stage-Level Atomicity: Any module failure halts the entire stage.
    - Deterministic Lineage: Uses run_id for output partitioning.

    Side Effects:
    - Creates semantic module subdirectories.
    - Generates a comprehensive stage report.

    Failure Behavior:
    - Returns a report with status='failed' if source files are missing or any module fails.

    Returns:
        Dict: A global report of module statuses and error logs.
    """

    report = init_report()
    report["modules"] = {}

    df_assembled = load_assembled_data(
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

    if "df_assembled" in locals():
        del df_assembled
    force_gc()

    return report
