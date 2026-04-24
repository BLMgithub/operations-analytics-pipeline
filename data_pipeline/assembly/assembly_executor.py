# =============================================================================
# Assemble Events Stage Executor
# =============================================================================

import gc
from typing import Dict
import ctypes
import platform
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import (
    load_historical_data,
    scan_gcs_uris_from_bigquery,
    export_file,
)
from data_pipeline.shared.modeling_configs import DIMENSION_REFERENCES
from data_pipeline.assembly.assembly_logic import (
    init_report,
    log_info,
    log_error,
    loaded_data,
    dimension_references,
    merge_data,
    derive_fields,
    freeze_schema,
    task_wrapper,
    load_event_table,
    export_path,
)


def force_gc():
    """
    Force the Linux allocator to release memory back to the OS.

    Workflow:
    1. Purge: Triggers Python's global garbage collection.
    2. Purge: Invokes libc malloc_trim if on a Linux system to reclaim heap memory.

    Operational Guarantees:
    - Memory Safety: Minimizes memory fragmentation and peak RSS.

    Side Effects:
    - Instructs the OS to reclaim unused memory from the process heap.
    """
    gc.collect()
    if platform.system() == "Linux":
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            print(f"[WARNING] Force gc and malloc_trim (release memory) failed: {e}")


# ------------------------------------------------------------
# EVENT ASSEMBLY ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_event_assembly(run_context: RunContext, report: Dict) -> bool:
    """
    Coordinates the linear transformation pipeline for order-grain events.

    Workflow:
    1. Hydrate: Fetches core event tables (Orders, Items, Payments).
    2. Delegate: Joins and aggregates into a single row-per-order grain.
    3. Delegate: Calculates analytical time-deltas and lineage.
    4. Validate: Enforces semantic schema and dtypes.
    5. Promote: Persists the unified event table to the assembly zone.
    6. Purge: Triggers explicit memory reclamation via force_gc().

    Operational Guarantees:
    - Grain Integrity: Strictly enforces one row per order_id.
    - Fail-Fast: Halts immediately if any sub-task fails.

    Side Effects:
    - Persists 'assembled_events' Parquet file.
    - Mutates 'report' with event assembly metrics.

    Failure Behavior:
    - Returns False and logs errors to the report upon any step failure.
    """

    report["assembled_events"] = {
        "merge_events": False,
        "derive_fields": False,
        "freeze_schema": False,
        "export": False,
    }

    tracker = report["assembled_events"]
    lf_freezed = None

    try:

        tables = load_event_table(run_context, report)

        if not tables:
            return False

        ok, lf_merged = task_wrapper(
            report=report,
            step_name="merge_events",
            status_tracker=tracker,
            func=merge_data,
            tables=tables,
        )
        if not ok:
            return False

        del tables
        force_gc()

        ok, lf_derived = task_wrapper(
            report=report,
            step_name="derive_fields",
            status_tracker=tracker,
            func=derive_fields,
            lf=lf_merged,
        )
        if not ok:
            return False

        ok, lf_freezed = task_wrapper(
            report=report,
            step_name="freeze_schema",
            status_tracker=tracker,
            func=freeze_schema,
            lf=lf_derived,
        )
        if not ok:
            return False

        path = export_path(run_context, "assembled_events")

        if not export_file(
            df=lf_freezed,
            output_path=path,
            log_error=lambda msg: log_error(msg, report),
        ):
            tracker["export"] = False
            return False

        tracker["export"] = True
        report["status"] = "success"
        log_info("Export: assembled_events successfully", report)

    except Exception as e:
        log_error(f"Unexpected error processing event assembly: {e}", report)
        report["status"] = "failed"
        return False

    finally:
        if "lf_derived" in locals():
            del lf_derived  # type: ignore
        if "lf_freezed" in locals():
            del lf_freezed
        force_gc()

    return True


# ------------------------------------------------------------
# DIMENSION REFERENCE ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_dimension_refs(run_context: RunContext, report: Dict) -> bool:
    """
    Iteratively extracts and exports dimension reference tables.

    Workflow:
    1. Hydrate: Loads historical source table from the contracted zone.
    2. Delegate: Extracts unique dimension keys and required columns.
    3. Promote: Persists the reference table to the assembly zone.
    4. Purge: Clears intermediate dataframes and triggers garbage collection.

    Operational Guarantees:
    - Fail-Fast: Termination of the entire loop if any dimension extraction fails.

    Side Effects:
    - Persists multiple dimension reference Parquet files.

    Failure Behavior:
    - Logs table-specific errors and returns False.
    """

    for table, config in DIMENSION_REFERENCES.items():

        report[table] = {"dim_reference": False, "export": False}
        tracker = report[table]

        try:
            # Switch between local and gcp IO
            if run_context.bq_project_id == "PROJECT_ID_NOT_DETECTED":
                lf_raw = load_historical_data(
                    base_path=run_context.storage_contracted_path, table_name=table
                )
            else:
                lf_raw = scan_gcs_uris_from_bigquery(
                    project_id=run_context.bq_project_id,
                    dataset_id=run_context.bq_dataset_id,
                    table_id=table,
                    log_info=lambda msg: loaded_data(msg, report),
                )

            if lf_raw is None:
                return False

            primary_key = config.get("primary_key", [])
            require_col = config.get("required_column", [])
            dtypes = config.get("dtypes", {})

            ok, df_dim = task_wrapper(
                report=report,
                step_name="dim_reference",
                status_tracker=tracker,
                func=dimension_references,
                lf=lf_raw,
                primary_key=primary_key,
                req_column=require_col,
                dtypes=dtypes,
            )

            if not ok:
                return False

            path = export_path(run_context, table)

            if not export_file(
                df=df_dim,
                output_path=path,
                log_error=lambda msg: log_error(msg, report),
            ):

                tracker["export"] = False
                return False

            tracker["export"] = True
            log_info(f"Export dimension reference:{table} successfully", report)

        except FileNotFoundError as e:
            log_error(f"File not found for dimension table {table}: {e}", report)

            return False

        except Exception as e:
            log_error(
                f"Unexpected error processing dimension table {table}: {e}", report
            )
            return False

        finally:
            if "lf_raw" in locals():
                del lf_raw  # type: ignore
            if "df_dim" in locals():
                del df_dim  # type: ignore
            gc.collect()

    return True


# ------------------------------------------------------------
# DATA ASSEMBLING
# ------------------------------------------------------------


def assemble_events(run_context: RunContext) -> dict:
    """
    Main entry point for the Silver-to-Gold Assembly stage.

    Workflow:
    1. Delegate: Triggers Workflow I (Event Assembly).
    2. Delegate: Triggers Workflow II (Dimension Reference Extraction).

    Operational Guarantees:
    - Sequential Dependency: Dimension references only process after event assembly attempts.
    - Stage Atomicity: Any failure in either workflow marks the stage as 'failed'.

    Side Effects:
    - Orchestrates the creation of the entire Gold/Assembled layer.

    Failure Behavior:
    - Returns a report with status='failed' if either orchestration branch returns False.

    Returns:
        dict: A stage report containing 'status' and step-level execution logs.
    """

    report = init_report()

    if not orchestrate_event_assembly(run_context, report):
        report["status"] = "failed"
        return report

    if not orchestrate_dimension_refs(run_context, report):
        report["status"] = "failed"
        return report

    return report
