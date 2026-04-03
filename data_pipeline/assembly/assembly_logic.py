# =============================================================================
# Assembly Events Stage logic
# =============================================================================

import polars as pl
from pathlib import Path
from typing import Dict, Callable, Any, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_historical_table
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
    Core event assembly join and grain enforcement using Hash-Join optimization.

    Contract:
    - Inner joins 'df_orders' with 'df_order_items' to ensure analytical relevance.
    - Left joins 'df_payments' to capture financial metadata.

    Optimization Logic:
    - Hash-Join: Maps high-cardinality UUIDs to UInt64 hashes to reduce Join Hash Table memory.
    - Pre-aggregation: Sums payments and deduplicates items BEFORE joining to guarantee
      a strict 1:1 grain and prevent Cartesian row explosions.
    - Early Projection: Selects required columns at the source to minimize join width.

    Invariants:
    - Dataset Grain: Strictly one row per 'order_id'.
    - Referential Integrity: Orders lacking corresponding item records are discarded.

    Failures:
    - Potential for cardinality explosion if pre-aggregation logic is bypassed.
    """

    pl.enable_string_cache()

    col_orders = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_timestamp",
        "order_estimated_delivery_date",
    ]

    # Pre-aggregate Tables
    lf_payments_agg = (
        tables["df_payments"]
        .with_columns(join_key=pl.col("order_id").hash())
        .group_by("join_key")
        .agg(order_revenue=pl.col("payment_value").sum())
    )

    lf_items_agg = (
        tables["df_order_items"]
        .with_columns(
            join_key=pl.col("order_id").hash(),
            product_id=pl.col("product_id").cast(pl.Categorical),
            seller_id=pl.col("seller_id").cast(pl.Categorical),
        )
        .group_by("join_key")
        .agg(
            product_id=pl.col("product_id").first(),
            seller_id=pl.col("seller_id").first(),
        )
    )

    lf_orders = (
        tables["df_orders"]
        .select(col_orders)
        .with_columns(
            join_key=pl.col("order_id").hash(),
            order_status=pl.col("order_status").cast(pl.Categorical),
        )
    )

    df_merged = (
        lf_orders.join(lf_items_agg, on="join_key", how="inner")
        .join(lf_payments_agg, on="join_key", how="left")
        .drop("join_key")
    )

    return df_merged


def derive_fields(lf: pl.LazyFrame, run_id: str) -> pl.LazyFrame:
    """
    Analytical enrichment and temporal metric derivation layer.

    Contract:
    - Calculates day-grain durations for fulfillment and approval latency.
    - Stays compliant with ISO-8601 for week/year attributes.

    Optimization Logic:
    - Memory-Efficient Casting: Forces durations and years to Int16 (2 bytes) to minimize row width.
    - Categorical Compression: Casts repetitive strings (year_week, run_id) to Categorical.
    - Early Drop: Purges non-contract columns (e.g., estimated_delivery) immediately after use.

    Invariants:
    - Lineage: Every row is stamped with the current 'run_id' for traceability.
    - Temporal Grain: Metrics (lead_time, lags, delays) are represented as integer days.

    Failures:
    - Raises ComputeError (from Polars) if date subtraction logic encounters nulls
      in non-nullable date columns.
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
        order_date=pl.col("order_purchase_timestamp").dt.date(),
        order_year=pl.col("order_purchase_timestamp").dt.year(),
        order_week_iso=pl.col("order_purchase_timestamp").dt.strftime("W%V"),
        order_year_week=pl.col("order_purchase_timestamp")
        .dt.strftime("%G-W%V")
        .cast(pl.Categorical),
        run_id=pl.lit(run_id).cast(pl.Categorical),
    ).drop("order_estimated_delivery_date")

    return lf_derived


def freeze_schema(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Finalizes the structural contract via schema projection and type casting.

    Contract:
    - Schema Projection: Drops all columns not explicitly defined in 'ASSEMBLE_SCHEMA'.
    - Type Enforcement: Casts remaining columns to the formats defined in 'ASSEMBLE_DTYPES'.

    Optimization Logic:
    - Zero-Copy Streaming: Omits sorting to maintain the non-blocking execution plan
      required for efficient sink_parquet() operations in memory-constrained environments.

    Invariants:
    - Structure: Final execution plan exactly matches the modeling configuration spec.

    Failures:
    - Raises RuntimeError if the input frame lacks columns required by 'ASSEMBLE_SCHEMA'.
    """
    current_columns = lf.collect_schema().names()

    missing_cols = set(ASSEMBLE_SCHEMA) - set(current_columns)
    if missing_cols:
        raise RuntimeError(f"missing required columns: {sorted(missing_cols)}")

    lf_contract = lf.select(ASSEMBLE_SCHEMA).cast(pl.Schema(ASSEMBLE_DTYPES))

    return lf_contract


# ------------------------------------------------------------
# SEMANTIC DIMENSION REFERENCE
# ------------------------------------------------------------


def dimension_references(
    lf: pl.LazyFrame,
    table_name: str,
    primary_key: list[str],
    req_column: list[str],
) -> pl.LazyFrame:
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

    lf_dim = lf.select(req_column).unique(subset=primary_key).sort(primary_key)

    if (
        lf_dim.select(pl.col(primary_key).is_duplicated().any())
        .collect(engine="streaming")
        .item()
    ):
        raise RuntimeError(f"Duplicated {primary_key} detected in {table_name}")

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
    1. Dispatch: Executes the logic function 'func' with provided arguments.
    2. Monitor: Traps any exceptions raised during execution.
    3. Report: Updates 'status_tracker' and logs success/failure to the report.

    Operational Guarantees:
    - Fail-Safe: Traps all exceptions to prevent pipeline termination, converting
      errors into 'failed' status reports.
    - Integrity: Updates the 'status' field in the report for the given step to
      either True (success) or False (failed) regardless of outcome.

    Side Effects:
    - Mutates 'report' and 'status_tracker' dictionaries in-place.
    - Prints informational/error logs to stdout.

    Failure Behavior:
    - Catches all Exceptions, logs the traceback via 'log_error', and returns
      (False, None) to signal a controlled sub-task failure.

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
        log_error(f"Step {step_name} failed: {(e)}", report)
        status_tracker[step_name] = False
        return False, None


# ------------------------------------------------------------
# IO Wrappers
# ------------------------------------------------------------


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
        try:
            df = load_historical_table(
                contracted_path,
                table_name,
                log_info=lambda msg: loaded_data(msg, report),
            )

            if df is not None:
                tables[table_name] = df

        except Exception as e:
            log_error(f"Required table {table_name} not found: {e}", report)
            return None

    if len(tables) < len(EVENT_TABLES):
        return None

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
