# =============================================================================
# Assembly Events Stage logic
# =============================================================================

import polars as pl
from pathlib import Path
from typing import Dict, Callable, Any, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import (
    load_historical_data,
    scan_gcs_uris_from_bigquery,
)
from data_pipeline.shared.modeling_configs import ASSEMBLE_SCHEMA, ASSEMBLE_DTYPES

EVENT_TABLES = ["df_orders", "df_order_items", "df_payments"]

# ------------------------------------------------------------
# ASSEMBLE REPORT & LOGS
# ------------------------------------------------------------


def init_report():
    return {
        "status": "",
        "errors": [],
        "info": [],
        "loaded_data": [],
    }


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_error(message: str, report: Dict[str, list[str]]) -> None:
    print(f"[ERROR] {message}")
    report["errors"].append(message)


def loaded_data(message: str, report: Dict[str, list[str]]) -> None:
    print(f"[INFO] {message}")
    report["loaded_data"].append(message)


# ------------------------------------------------------------
# ASSEMBLY AND SCHEMA ENFORCEMENT
# ------------------------------------------------------------


def merge_data(tables: Dict) -> pl.LazyFrame:
    """
    Core event assembly and grain enforcement using the Primitive Integer Pipeline.

    Contract:
    - Integer-Join: Leverages pre-mapped UInt32/UInt64 IDs (order_id_int) to execute memory-efficient joins.
    - Grain Enforcement: Ensures a strict 1:1 analytical grain through pre-aggregation of children.

    Optimization Logic:
    - Primitive Integer Pipeline: Eliminates 36-byte UUID string overhead in Hash Tables, reducing memory footprint by >60%.
    - Early Projection: Selects required columns at the source to minimize join width.

    Invariants:
    - Dataset Grain: Strictly one row per 'order_id_int'.

    Outputs:
    - Merged LazyFrame containing joined order, item, and payment data.

    Failures:
    - [Structural] Crashes if required tables ('df_orders', 'df_order_items') are missing from input Dict.
    """

    pl.enable_string_cache()

    col_orders = [
        "order_id_int",
        "customer_id_int",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_timestamp",
        "order_estimated_delivery_date",
    ]

    # Pre-aggregate Tables
    lf_payments_agg = (
        tables["df_payments"]
        .group_by("order_id_int")
        .agg(order_revenue=pl.col("payment_value").sum())
    )

    lf_items_agg = (
        tables["df_order_items"]
        .select(["order_id_int", "product_id_int", "seller_id_int"])
        .group_by("order_id_int")
        .agg(
            product_id_int=pl.col("product_id_int").first(),
            seller_id_int=pl.col("seller_id_int").first(),
        )
    )

    lf_orders = (
        tables["df_orders"]
        .select(col_orders)
        .with_columns(
            order_status=pl.col("order_status").cast(pl.Categorical),
        )
    )

    df_merged = lf_orders.join(lf_items_agg, on="order_id_int", how="inner").join(
        lf_payments_agg, on="order_id_int", how="left"
    )

    return df_merged


def derive_fields(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Analytical enrichment and temporal metric derivation layer.

    Contract:
    - Calculates day-grain durations for fulfillment and approval latency.
    - Stays compliant with ISO-8601 for week/year attributes.

    Optimization Logic:
    - Memory-Efficient Casting: Forces durations and years to Int16 (2 bytes) to minimize row width.
    - Categorical Compression: Casts repetitive strings (year_week) to Categorical.
    - Early Drop: Purges non-contract columns (e.g., estimated_delivery) immediately after use.

    Invariants:
    - Temporal Grain: Metrics (lead_time, lags, delays) are represented as integer days.

    Outputs:
    - Enriched LazyFrame with derived analytical fields.

    Failures:
    - [Structural] Crashes if input LazyFrame lacks required timestamp columns.
    """

    lf_derived = lf.with_columns(
        lead_time_days=(
            pl.col("order_delivered_timestamp") - pl.col("order_approved_at")
        )
        .dt.total_days()
        .cast(pl.Int16),
        approval_lag_days=(
            pl.col("order_approved_at") - pl.col("order_purchase_timestamp")
        )
        .dt.total_days()
        .cast(pl.Int16),
        delivery_delay_days=(
            pl.col("order_delivered_timestamp")
            - pl.col("order_estimated_delivery_date")
        )
        .dt.total_days()
        .cast(pl.Int16),
        order_date=pl.col("order_purchase_timestamp").dt.date().cast(pl.Datetime("us")),
        order_year_week=pl.col("order_purchase_timestamp")
        .dt.strftime("%G-W%V")
        .cast(pl.Categorical),
    ).drop("order_estimated_delivery_date")

    return lf_derived


def freeze_schema(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Finalizes the structural contract via schema projection and type casting.

    Contract:
    - Schema Projection: Drops all columns not explicitly defined in 'ASSEMBLE_SCHEMA'.
    - Type Enforcement: Casts remaining columns to the formats defined in 'ASSEMBLE_DTYPES'.

    Optimization Logic:
    - Zero-Copy Streaming: Omits sorting to maintain the non-blocking execution plan required for efficient sink_parquet() operations.

    Invariants:
    - Structure: Final execution plan exactly matches the modeling configuration spec.

    Outputs:
    - Schema-compliant LazyFrame ready for persistence.

    Failures:
    - [Structural] Raises RuntimeError if input frame lacks columns required by 'ASSEMBLE_SCHEMA'.
    """

    current_columns = lf.collect_schema().names()

    missing_cols = set(ASSEMBLE_SCHEMA) - set(current_columns)
    if missing_cols:
        raise RuntimeError(f"missing required columns: {sorted(missing_cols)}")

    lf_contract = lf.select(ASSEMBLE_SCHEMA)

    datetime_cols = [
        col for col, dtype in ASSEMBLE_DTYPES.items() if isinstance(dtype, pl.Datetime)
    ]

    lf_contract = lf_contract.with_columns(
        [pl.col(col).dt.cast_time_unit("us") for col in datetime_cols]
    )

    return lf_contract.cast(pl.Schema(ASSEMBLE_DTYPES))


# ------------------------------------------------------------
# SEMANTIC DIMENSION REFERENCE
# ------------------------------------------------------------


def dimension_references(
    lf: pl.LazyFrame,
    primary_key: list[str],
    req_column: list[str],
    dtypes: dict,
) -> pl.LazyFrame:
    """
    Extracts a unique reference dataset from a historical source.

    Contract:
    - Subtractive Filtering: Selects specified 'req_column' set and enforces uniqueness.
    - Type Enforcement: Casts columns to the formats defined in the provided 'dtypes' schema.

    Invariants:
    - Dataset Grain: Strictly one row per 'primary_key'.

    Outputs:
    - Unique reference LazyFrame.

    Failures:
    - [Structural] Crashes if input LazyFrame lacks 'primary_key' or 'req_column'.
    """

    lf_dim = lf.select(req_column).unique(subset=primary_key).cast(pl.Schema(dtypes))

    return lf_dim


# ------------------------------------------------------------
# TASK WRAPPER
# ------------------------------------------------------------


def task_wrapper(
    report: dict,
    step_name: str,
    status_tracker: dict,
    func: Callable,
    *args,
    **kwargs,
) -> tuple[bool, Any]:
    """
    Unified task runner that handles logging, reporting, and execution.

    Workflow:
    1. Delegate: Executes the logic function 'func' with provided arguments.
    2. Monitor: Traps any exceptions raised during execution.
    3. Promote: Updates 'status_tracker' and logs success/failure to the report.

    Operational Guarantees:
    - Fail-Safe: Traps all exceptions to prevent pipeline termination.
    - Integrity: Updates step-level status regardless of outcome.

    Side Effects:
    - Mutates 'report' and 'status_tracker' dictionaries in-place.
    - Prints informational/error logs to stdout.

    Failure Behavior:
    - Returns (False, None) upon any exception.

    Returns:
        tuple[bool, Any]: (Success Boolean, Result Data or None).
    """

    try:
        result = func(*args, **kwargs)

        if result is None:
            status_tracker[step_name] = False
            return False, None

        status_tracker[step_name] = True
        print(f"[INFO] Step {step_name} completed successfully.")
        return True, result

    except Exception as e:
        log_error(f"Step {step_name} failed: {e}", report)
        status_tracker[step_name] = False
        return False, None


# ------------------------------------------------------------
# IO Wrappers
# ------------------------------------------------------------


def load_event_table(run_context: RunContext, report: Dict) -> Any:
    """
    Batch-loads core event tables required for assembly from BigQuery.

    Contract:
    - Hydrate: Iterates through EVENT_TABLES and streams data via scan_gcs_uris_from_bigquery.

    Outputs:
    - Dict keyed by table name containing loaded LazyFrames.

    Failures:
    - [Operational] Returns None if any required table is missing or fails to load.
    """

    tables = {}

    for table_name in EVENT_TABLES:
        try:
            # Switch between local and gcp IO
            if run_context.bq_project_id == "PROJECT_ID_NOT_DETECTED":
                df = load_historical_data(
                    base_path=run_context.storage_contracted_path,
                    table_name=table_name,
                    log_info=lambda msg: loaded_data(msg, report),
                )

            else:
                df = scan_gcs_uris_from_bigquery(
                    project_id=run_context.bq_project_id,
                    dataset_id=run_context.bq_dataset_id,
                    table_id=table_name,
                    log_info=lambda msg: loaded_data(msg, report),
                )

            if df is not None:
                tables[table_name] = df

        except Exception as e:
            log_error(f"Required table {table_name} not found : {e}", report)
            return None

    if len(tables) < len(EVENT_TABLES):
        return None

    return tables


def export_path(run_context: RunContext, file_name: str) -> Path:
    """
    Generates a deterministic destination path for assembled artifacts.

    Contract:
    - Transformation: Parses 'run_id' to extract date partitions and constructs a timestamped filename.

    Invariants:
    - Path Integrity: Output location is always relative to 'run_context.assembled_path'.

    Outputs:
    - Path object for the target file.

    Failures:
    - [Structural] Crashes if 'run_id' format is invalid.
    """

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    file_name = f"{file_name}_{year}_{month}_{day}.parquet"
    output_path = run_context.assembled_path / file_name

    return output_path
