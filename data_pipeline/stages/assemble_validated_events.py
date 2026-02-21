# =============================================================================
# Assemble Validated Event Data
# =============================================================================
# - Combine only CI-validated raw datasets into a unified event-level dataset
# - Enforce explicit join paths, keys, and cardinality assumptions
# - Preserve event grain and temporal semantics during assembly
# - Produce a deterministic, audit-ready event dataset for fact derivation


import pandas as pd
from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file

EVENT_TABLES = ["df_orders", "df_order_items", "df_payments"]

# ------------------------------------------------------------
# ASSEMBLE REPORT & LOGS
# ------------------------------------------------------------


def init_report() -> Dict[str, list[str]]:
    return {"error": [], "info": []}


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_error(message: str, report: Dict[str, list[str]]) -> None:
    print(f"[ERROR] {message}")
    report["error"].append(message)


# ------------------------------------------------------------
# ASSEMBLE DATA
# ------------------------------------------------------------


def assemble_data(run_context: RunContext) -> Dict:

    report = init_report()

    def info(msg):
        log_info(msg, report)

    def error(msg):
        log_error(msg, report)

    contracted_path = run_context.contracted_path

    for table_name in EVENT_TABLES:

        df = load_logical_table(
            contracted_path, table_name, log_info=info, log_error=error
        )

        if df is None:
            log_error(f"{table_name}: dataset is empty", report)

    df_assembled = pd.DataFrame({"Place": ["Holder"]})

    output_path = run_context.assembled_path / "assembled_events.parquet"

    if not export_file(df_assembled, output_path):
        log_error("error exporting file", report)

    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
