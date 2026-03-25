# =============================================================================
# Assemble Events Stage Executor
# =============================================================================

import gc
from typing import Dict
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table, export_file
from data_pipeline.assembly.assembly_logic import (
    init_report,
    DIMENSION_REFERENCES,
    dimension_references,
    merge_data,
    derive_fields,
    freeze_schema,
    load_event_table,
    task_wrapper,
    export_path,
)


def init_stage_report():
    return {
        "status": "success",
        "steps": {
            "load_tables": init_report(),
            "merge_events": init_report(),
            "derive_fields": init_report(),
            "freeze_schema": init_report(),
            "dim_reference": init_report(),
            "export": init_report(),
        },
    }


# ------------------------------------------------------------
# EVENT ASSEMBLY ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_event_assembly(run_context: RunContext, report: Dict) -> bool:
    """Handles the heavy lifting of the Silver-to-Gold event merge."""

    tables = load_event_table(run_context, report["steps"]["load_tables"])
    if not tables:
        return False

    # Merging data
    ok, df = task_wrapper("merge_events", report, merge_data, tables)
    if not ok:
        return False
    del tables

    # Derive fields
    ok, df = task_wrapper(
        "derive_fields", report, derive_fields, df, run_context.run_id
    )
    if not ok:
        return False

    # Freeze schema
    ok, df = task_wrapper("freeze_schema", report, freeze_schema, df)
    if not ok:
        return False

    # Export Assembled events
    path = export_path(run_context, "assembled_events")
    if not export_file(df, path):
        return False

    gc.collect()
    return True


# ------------------------------------------------------------
# DIMENSION REFERENCE ORCHESTRATION
# ------------------------------------------------------------


def orchestrate_dimension_refs(run_context: RunContext, report: Dict) -> bool:
    """Handles the extraction and export of dimension reference tables."""

    for table, config in DIMENSION_REFERENCES.items():
        df_raw = load_historical_table(run_context.contracted_path, table)
        if df_raw is None:
            return False

        ok, df_dim = task_wrapper(
            "dim_reference",
            report,
            dimension_references,
            df_raw,
            table,
            config["primary_key"],
            config["required_column"],
        )

        if not ok:
            return False

        # Export
        path = export_path(run_context, table)
        if not export_file(df_dim, path):
            return False

        del df_raw, df_dim
        gc.collect()

    return True


# ------------------------------------------------------------
# DATA ASSEMBLING
# ------------------------------------------------------------


def assemble_events(run_context: RunContext) -> dict:
    """
    Assemble contract-compliant event dataset (order grain).

    Event Assembly Steps:
    - Load contracted event tables
    - Merge with cardinality enforcement (1 row per order_id)
    - Derive temporal metrics and lineage fields
    - Freeze schema and enforce dtypes
    - Export Assembly output

    Dimension Reference Steps:
    -
    -
    - Export dimension reference output

    Grain:
    - One row per order_id (hard fail on violation)
    """

    report = init_stage_report()

    if not orchestrate_event_assembly(run_context, report):
        report["status"] = "failed"
        return report

    if not orchestrate_dimension_refs(run_context, report):
        report["status"] = "failed"
        return report

    return report
