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
    Core event assembly join and grain enforcement.

    Contract:
    - Inner joins 'df_orders' with 'df_order_items' to ensure analytical relevance.
    - Left joins 'df_payments' to capture financial metadata.
    - Projects 'payment_value' as 'order_revenue'.

    Invariants:
    - Dataset Grain: Strictly one row per 'order_id'.
    - Referential Integrity: Orders lacking corresponding item records are discarded.

    Failures:
    - Raises RuntimeError if a 1-to-Many cardinality explosion is detected (duplicate order_ids).
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
    Analytical enrichment and temporal metric derivation layer.

    Contract:
    - Standardizes core lifecycle timestamps to datetime objects.
    - Calculates day-grain durations for fulfillment and approval latency.
    - Stays compliant with ISO-8601 for week/year attributes.

    Invariants:
    - Lineage: Every row is stamped with the current 'run_id' for traceability.
    - Temporal Grain: Metrics (lead_time, lags, delays) are represented as integer days.

    Failures:
    - [Undetermined] - Assumes source timestamps have passed the 'remove_unparsable_timestamps' contract.
    """

    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_timestamp",
        "order_estimated_delivery_date",
    ]:
        df[col] = pd.to_datetime(df[col])

    df["lead_time_days"] = (
        df["order_delivered_timestamp"] - df["order_approved_at"]
    ).dt.days

    df["approval_lag_days"] = (
        df["order_approved_at"] - df["order_purchase_timestamp"]
    ).dt.days

    df["delivery_delay_days"] = (
        df["order_delivered_timestamp"] - df["order_estimated_delivery_date"]
    ).dt.days

    df["order_date"] = df["order_purchase_timestamp"].dt.date
    df["order_year"] = df["order_purchase_timestamp"].dt.year
    df["order_week_iso"] = df["order_purchase_timestamp"].dt.strftime("W%V")
    df["order_year_week"] = df["order_purchase_timestamp"].dt.strftime("%G-W%V")
    df["run_id"] = run_id

    return df


def freeze_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final technical contract enforcement and semantic projection.

    Contract:
    - Prunes all intermediate columns, retaining only the 'ASSEMBLE_SCHEMA' subset.
    - Enforces strict type casting via 'ASSEMBLE_DTYPES'.
    - Applies deterministic sorting and index resetting.

    Invariants:
    - Sorting: Guaranteed ascending order by 'order_id'.
    - Structure: Final frame exactly matches the modeling configuration spec.

    Failures:
    - Raises RuntimeError if the input frame lacks columns required by 'ASSEMBLE_SCHEMA'.
    """

    missing_cols = set(ASSEMBLE_SCHEMA) - set(df.columns)
    if missing_cols:
        raise RuntimeError(f"missing required columns: {sorted(missing_cols)}")

    df_contract = df[ASSEMBLE_SCHEMA]
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
    Extracts a unique reference dataset from a historical source.

    Contract:
    - Filters input to specified 'req_column' set.
    - Enforces uniqueness based on 'primary_key'.

    Invariants:
    - Dataset Grain: Strictly one row per 'primary_key'.
    - Sorting: Inherits source order (deterministic behavior not guaranteed).

    Failures:
    - Raises RuntimeError if 'primary_key' duplicates persist after extraction.
    """

    df_dim = df[req_column].drop_duplicates(subset=primary_key)

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

    Contract:
    - Encapsulates execution of a logic function with standardized telemetry.
    - Manages the state transition of the 'report' object for the specific 'step_name'.

    Inputs:
    - step_name: The lookup key in the report['steps'] dictionary.
    - report: The shared state dictionary initialized by 'init_stage_report'.
    - func: The logic/transformation function to be executed.
    - *args: Positional arguments passed directly to 'func'.

    Outputs:
    - Returns a tuple of (Success Boolean, Result Data).
    - Result Data is 'None' if the task fails or returns no data.

    Invariants:
    - Fail-Safe: Traps all exceptions to prevent pipeline termination, converting errors into 'failed' status reports.
    - Integrity: Updates the 'status' field in the report for the given step to either 'success' or 'failed' regardless of outcome.
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
    """
    Batch-loads core event tables required for assembly.

    Contract:
    - Iterates through the global EVENT_TABLES registry.
    - Loads Parquet files from the provided 'contracted_path'.

    Outputs:
    - Returns a dictionary keyed by table name.
    """

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
    """
    Generates a deterministic destination path for assembled artifacts.

    Contract:
    - Parses the 'run_id' (format: YYYYMMDD...) to extract date partitions.
    - Constructs a filename with the pattern: {file_name}_{YYYY}_{MM}_{DD}.parquet.

    Invariants:
    - Output location is always relative to 'run_context.assembled_path'.
    """

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    file_name = f"{file_name}_{year}_{month}_{day}.parquet"
    output_path = run_context.assembled_path / file_name

    return output_path
