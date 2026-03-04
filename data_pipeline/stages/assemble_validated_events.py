# =============================================================================
# Assemble Validated Event Data
# =============================================================================
# - Combine validated raw datasets into a unified event-level dataset
# - Enforce explicit join paths, keys, and cardinality assumptions
# - Preserve event grain and temporal semantics during assembly
# - Produce a deterministic, audit-ready event dataset for fact derivation


import pandas as pd
from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.modeling_configs import (
    ASSEMBLE_SCHEMA,
    ASSEMBLE_DTYPES,
)
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file

EVENT_TABLES = ["df_orders", "df_order_items", "df_payments"]

# ------------------------------------------------------------
# ASSEMBLE REPORT & LOGS
# ------------------------------------------------------------


def init_report():
    return {"status": "success", "errors": [], "info": []}


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_error(message: str, report: Dict[str, list[str]]) -> None:
    print(f"[ERROR] {message}")
    report["errors"].append(message)


# ------------------------------------------------------------
# ASSEMBLY AND SCHEMA ENFORCEMENT
# ------------------------------------------------------------


def merge_data(tables: dict) -> pd.DataFrame:
    """
    Core event assembly join.

    Combines order header, order items, and payment tables into a
    single event-grain dataset keyed at one row per order.

    Structural expectations:
    - `df_orders` defines the base order grain
    - `df_order_items` must not expand order cardinality
    - `df_payments` is left-joined, expected one payment per order
    """

    df_orders = tables["df_orders"]
    df_order_items = tables["df_order_items"]
    df_payments = tables["df_payments"]

    df_merged = df_orders.merge(df_order_items, on="order_id", how="inner").merge(
        df_payments, on="order_id", how="left"
    )

    df_merged = df_merged.rename(columns={"payment_value": "order_revenue"})

    if len(df_merged) != len(df_orders):
        raise RuntimeError("Cardinality violation detected: expected 1 row per order")

    return df_merged


def derive_fields(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """
    Event enrichment layer.

    Standardizes timestamp types and derives analytical time fields
    required for downstream semantic modeling.

    Derived fields:
    - `lead_time_days` - approval to delivery duration
    - `approval_lag_days` - purchase to approval duration
    - `delivery_delay_days` - actual vs estimated delivery gap
    - `order_date`, `order_year`, ISO week attributes
    - `run_id` lineage stamp for run traceability

    Behavior:
    - Casts required timestamp columns to datetime
    - Performs duration calculations in day grain
    """

    df_derived = df.copy()

    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_timestamp",
        "order_estimated_delivery_date",
    ]:
        df_derived[col] = pd.to_datetime(df_derived[col])

    df_derived["lead_time_days"] = (
        df_derived["order_delivered_timestamp"] - df_derived["order_approved_at"]
    ).dt.days

    df_derived["approval_lag_days"] = (
        df_derived["order_approved_at"] - df_derived["order_purchase_timestamp"]
    ).dt.days

    df_derived["delivery_delay_days"] = (
        df_derived["order_delivered_timestamp"]
        - df_derived["order_estimated_delivery_date"]
    ).dt.days

    df_derived["order_date"] = df_derived["order_purchase_timestamp"].dt.date
    df_derived["order_year"] = df_derived["order_purchase_timestamp"].dt.year
    df_derived["order_week_iso"] = df_derived["order_purchase_timestamp"].dt.strftime(
        "W%V"
    )
    df_derived["order_year_week"] = df_derived["order_purchase_timestamp"].dt.strftime(
        "%G-W%V"
    )
    df_derived["run_id"] = run_id

    return df_derived


def freeze_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final structural contract enforcement.

    Projects the assembled dataset into the approved semantic schema,
    enforces critical dtypes, and applies deterministic ordering.

    Enforcement actions:
    - Selects only contract-approved columns
    - Casts required fields to declared dtypes
    - Sorts by `order_id` for stable downstream consumption
    - Resets index to produce a clean output frame
    """

    missing_cols = set(ASSEMBLE_SCHEMA) - set(df.columns)
    if missing_cols:
        raise RuntimeError(f"missing required columns: {sorted(missing_cols)}")

    df_contract = df[ASSEMBLE_SCHEMA].copy()
    df_contract = df_contract.astype(ASSEMBLE_DTYPES)
    df_contract = df_contract.sort_values("order_id").reset_index(drop=True)

    return df_contract


# ------------------------------------------------------------
# DATA ASSEMBLING
# ------------------------------------------------------------


def assemble_events(run_context: RunContext) -> Dict:
    """
    Assemble contract-compliant event dataset (order grain).

    Steps:
    - Load contracted event tables
    - Merge with cardinality enforcement (1 row per order_id)
    - Derive temporal metrics and lineage fields
    - Freeze schema and enforce dtypes
    - Export deterministic output

    Grain:
    - One row per order_id (hard fail on violation)
    """

    report = init_report()
    report["status"] = "success"

    def info(msg):
        log_info(msg, report)

    def error(msg):
        log_error(msg, report)

    contracted_path = run_context.contracted_path
    tables = {}

    for table_name in EVENT_TABLES:

        df = load_logical_table(
            contracted_path,
            table_name,
            log_info=info,
            log_error=error,
        )

        if df is None:
            error(f"{table_name}: dataset is empty")
            report["status"] = "failed"

            return report

        tables[table_name] = df

    try:
        df_merged = merge_data(tables)
        df_assembled = derive_fields(df_merged, run_context.run_id)
        df_contract = freeze_schema(df_assembled)

    except Exception as e:
        error(str(e))
        report["status"] = "failed"

        return report

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]

    output_path = (
        run_context.assembled_path / f"assembled_events_{year}_{month}.parquet"
    )

    if not export_file(df_contract, output_path):
        error("Export failed")
        report["status"] = "failed"

    info(
        f"Export success: assembled_events_{year}_{month}.parquet ({len(df_contract)} rows)"
    )
    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
