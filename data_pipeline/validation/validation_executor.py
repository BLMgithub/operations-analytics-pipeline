# =============================================================================
# Validation Stage Executor
# =============================================================================

from typing import Dict
import pandas as pd
from pathlib import Path
from data_pipeline.shared.loader_exporter import load_single_delta
from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from data_pipeline.validation.validation_logic import (
    init_report,
    log_info,
    log_error,
    run_base_validations,
    run_event_fact_validations,
    run_transaction_detail_validations,
    run_cross_table_validations,
)


def apply_validation(run_context: RunContext, base_path: Path | None = None) -> Dict:
    """
    Main entry point for the Pipeline Validation Stage.

    This component serves as the primary diagnostic gate for the data pipeline,
    ensuring that raw snapshots meet the structural requirements for the
    subsequent Contract and Assembly stages.

    Workflow:
        1. Loading: Iteratively fetches logical tables from the snapshot zone.
        2. Base Check: Enforces schema, uniqueness, and null constraints via 'run_base_validations'.
        3. Role Dispatch: Executes specialized logic (Event/Transaction) based on 'TABLE_CONFIG'.
        4. Referential Check: Evaluates inter-table integrity (orphans) via 'run_cross_table_validations'.

    Operational Guarantees:
    - Diagnostic Only: This function is read-only and will never mutate the source data.
    - Comprehensive Reporting: Captures all failures across all tables before returning; does not fail-fast on the first table error.
    - Severity: Structural issues are logged as 'errors' while referential issues are 'warnings'.

    Failure Behavior:
    - Non-Blocking: Continues processing remaining tables even if one fails base validations.
    - Status Update: Sets global report status to 'failed' if any errors or warnings are accumulated.

    Returns:
        Dict: A unified validation report containing 'status' and detailed finding lists.
    """

    if base_path is None:
        base_path = run_context.raw_snapshot_path

    report = init_report()

    tables: Dict[str, pd.DataFrame] = {}
    loaded_table_names = set()

    # Get assigned table configs
    for table_name, config in TABLE_CONFIG.items():

        df, _ = load_single_delta(
            base_path=base_path,
            table_name=table_name,
            log_info=lambda msg: log_info(msg, report),
        )

        if df is None:
            log_error(f"{table_name} logical table is missing", report)
            continue

        loaded_table_names.add(table_name)
        tables[table_name] = df

        if not run_base_validations(
            df,
            table_name,
            config["primary_key"],
            config["required_column"],
            config["non_nullable_column"],
            report,
        ):
            continue

        if config["role"] == "event_fact":
            run_event_fact_validations(df, table_name, report)

        elif config["role"] == "transaction_detail":
            run_transaction_detail_validations(df, table_name, report)

    expected_tables = set(TABLE_CONFIG.keys())

    missing_tables = sorted(expected_tables - loaded_table_names)
    if missing_tables:
        log_error(f"missing expected table(s) {missing_tables}", report)

    run_cross_table_validations(tables, report)

    if len(report["warnings"] or report["errors"]) > 0:
        report["status"] = "failed"

    return report
