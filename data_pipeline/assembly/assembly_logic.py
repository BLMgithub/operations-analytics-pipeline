# =============================================================================
# Assembly Events Stage logic
# =============================================================================

import pandas as pd
from pathlib import Path
from typing import Dict, Callable, Any, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table
from data_pipeline.shared.modeling_configs import ASSEMBLE_SCHEMA, ASSEMBLE_DTYPES

EVENT_TABLES = ["df_orders", "df_order_items", "df_payments"]

DIMENSION_REFERENCES = {
    "df_customers": {
        "primary_key": ["customer_id"],
        "required_column": [
            "customer_id",
            "customer_state",
        ],
    },
    "df_products": {
        "primary_key": ["product_id"],
        "required_column": [
            "product_id",
            "product_category_name",
            "product_weight_g",
        ],
    },
}

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


def merge_data(tables: Dict) -> pd.DataFrame:
    """
    Core event assembly join.

    Combines order header, order items, and payment tables into a
    single event-grain dataset keyed at one row per order.

    Structural expectations:
    - `df_orders` defines the base order grain
    - `df_order_items` must maintain 1:1 relationship with order_id
    - `df_payments` is left-joined, expected one payment per order
    """

    df_orders = tables["df_orders"]
    df_order_items = tables["df_order_items"]
    df_payments = tables["df_payments"]

    initial_orders = len(df_orders)

    df_merged = df_orders.merge(df_order_items, on="order_id", how="inner").merge(
        df_payments, on="order_id", how="left"
    )

    df_merged = df_merged.rename(columns={"payment_value": "order_revenue"})

    if df_merged["order_id"].duplicated().any():
        raise RuntimeError(
            "Cardinality violation: 1-to-Many explosion detected (multiple items/payments per order)"
        )

    if len(df_merged) < initial_orders:
        dropped_count = initial_orders - len(df_merged)
        print(
            f"[WARNING] Assembly: {dropped_count} orders dropped during inner join because they lacked valid order_items."
        )

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
# SEMANTIC DIMENSION REFERENCE
# ------------------------------------------------------------


def dimension_references(
    df: pd.DataFrame,
    table_name: str,
    primary_key: list[str],
    req_column: list[str],
) -> pd.DataFrame:
    """
    docstring.... returns dimension references
    """

    df_dim = df[req_column].drop_duplicates(subset=primary_key).copy()

    if df_dim[primary_key].duplicated().any():
        raise RuntimeError(f"Duplicated {primary_key} detected in {table_name}")

    return df_dim


# ------------------------------------------------------------
# TASK WRAPPERS
# ------------------------------------------------------------


def task_wrapper(
    step_name: str,
    report: Dict,
    func: Callable,
    *args,
) -> tuple[bool, Any]:
    """
    Unified task runner that handles logging, reporting, and execution.

    Returns:
        Success Boolean, Result Data
    """
    step_report = report["steps"][step_name]

    try:
        result = func(*args)

        if result is None:
            step_report["status"] = "failed"
            return False, None

        step_report["status"] = "success"
        log_info(f"Step {step_name} completed successfully.", step_report)
        return True, result

    except Exception as e:
        log_error(f"Step {step_name} failed: {str(e)}", step_report)
        step_report["status"] = "failed"

        return False, None


def load_event_table(run_context: RunContext, report: Dict) -> Any:

    contracted_path = run_context.contracted_path
    tables = {}

    for table_name in EVENT_TABLES:
        df = load_historical_table(
            contracted_path,
            table_name,
            log_info=lambda msg: log_info(msg, report),
        )

        if df is not None:
            tables[table_name] = df

    return tables


def export_path(run_context: RunContext, file_name: str) -> Path:
    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    file_name = f"{file_name}_{year}_{month}_{day}.parquet"
    output_path = run_context.assembled_path / file_name

    return output_path
