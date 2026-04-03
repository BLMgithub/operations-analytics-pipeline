# =============================================================================
# Assemble Events Stage Executor
# =============================================================================

import gc
from typing import Dict
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table, export_file
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


# ------------------------------------------------------------
# EVENT ASSEMBLY ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_event_assembly(run_context: RunContext, report: Dict) -> bool:
    """
    Coordinates the linear transformation pipeline for order-grain events.

    Execution Flow:
        1. Load: Fetch 'orders', 'items', and 'payments'.
        2. Merge: Join into a single row-per-order grain.
        3. Derive: Calculate analytical time-deltas and lineage.
        4. Freeze: Enforce semantic schema and dtypes.
        5. Export: Persist to the assembly zone.

    Memory Management:
    - Explicitly deletes intermediate DataFrames and triggers gc.collect()
      after export to minimize peak memory footprint.

    Failures:
    - Returns False immediately (fail-fast) if any sub-task wrapper fails.

    Failure Behavior:
    - Traps unexpected exceptions during the assembly pipeline via a try-except block.
    - Logs errors to the report and ensures memory reclamation via a finally block.
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

        ok, lf_derived = task_wrapper(
            report=report,
            step_name="derive_fields",
            status_tracker=tracker,
            func=derive_fields,
            lf=lf_merged,
            run_id=run_context.run_id,
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

    finally:
        gc.collect()

    return True


# ------------------------------------------------------------
# DIMENSION REFERENCE ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_dimension_refs(run_context: RunContext, report: Dict) -> bool:
    """
    Iteratively extracts and exports dimension reference tables.

    Contract:
    - Processes every table defined in the DIMENSION_REFERENCES registry.
    - Performs one-to-one extraction from Silver (contracted) to Gold (assembled).

    Invariants:
    - Fail-Fast: If a single dimension fails to load or validate, the
      entire orchestration terminates and returns False.

    Side Effects:
    - Performs per-iteration memory cleanup (del/gc.collect) to prevent
      accumulation of large dimension frames.

    Failure Behavior:
    - Traps FileNotFoundError and general Exceptions during each iteration.
    - Logs specific table-level failures and terminates the orchestration to
      maintain consistency across dimension snapshots.
    """

    for table, config in DIMENSION_REFERENCES.items():

        report[table] = {"dim_reference": False, "export": False}
        tracker = report[table]

        lf_raw = None
        df_dim = None

        try:
            lf_raw = load_historical_table(
                run_context.contracted_path,
                table,
                log_info=lambda msg: loaded_data(msg, report),
            )

            if lf_raw is None:
                return False

            primary_key = config.get("primary_key", [])
            require_col = config.get("required_column", [])

            ok, df_dim = task_wrapper(
                report=report,
                step_name="dim_reference",
                status_tracker=tracker,
                func=dimension_references,
                lf=lf_raw,
                table_name=table,
                primary_key=primary_key,
                req_column=require_col,
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
            log_error(f"File not found for dimension table {table}: {str(e)}", report)

            return False

        except Exception as e:
            log_error(
                f"Unexpected error processing dimension table {table}: {str(e)}", report
            )
            return False

        finally:
            if lf_raw is not None:
                del lf_raw
            if df_dim is not None:
                del df_dim
            gc.collect()

    return True


# ------------------------------------------------------------
# DATA ASSEMBLING
# ------------------------------------------------------------


def assemble_events(run_context: RunContext) -> dict:
    """
    Main entry point for the Silver-to-Gold Assembly stage.

    This component coordinates the transformation of normalized relational
    tables into contract-compliant analytical datasets.

    Workflow I: Event Assembly (Order Grain)
        1. Load: Fetches core event tables (Orders, Items, Payments).
        2. Merge: Join datasets with strict 1:1 order_id cardinality enforcement.
        3. Derive: Calculate temporal metrics (lead times) and lineage attributes.
        4. Freeze: Project final schema and enforce strictly defined dtypes.
        5. Export: Persist the unified event table to the Gold zone.

    Workflow II: Dimension Reference Extraction
        1. Iterate: Process Customer and Product registries.
        2. Extract: Select required columns and deduplicate by primary key.
        3. Export: Persist independent reference tables to the Gold zone.

    Operational Guarantees:
    - Grain: Strictly one row per 'order_id' for the event dataset.
    - Failure: Fail-fast; any task failure halts the stage and returns a 'failed' status.
    - Context: Relies on 'run_context' for deterministic path resolution.

    Failure Behavior:
    - Cascades orchestration failures: If either Workflow I or Workflow II returns
      False, the stage status is set to 'failed' and the report is returned immediately.

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
