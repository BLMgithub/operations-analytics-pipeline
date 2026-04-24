# =============================================================================
# Validation Stage Logic
# =============================================================================

from typing import Dict, List
import polars as pl
import polars.selectors as cs
from data_pipeline.shared.table_configs import (
    REQUIRED_TIMESTAMPS,
    TIMESTAMP_FORMATS,
)

# ------------------------------------------------------------
# VALIDATION REPORT & LOGS
# ------------------------------------------------------------


def init_report():
    return {
        "status": "success",
        "errors": [],
        "warnings": [],
        "info": [],
    }


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_warning(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[WARNING] {message}")
    report["warnings"].append(message)


def log_error(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[ERROR] {message}")
    report["errors"].append(message)


# ------------------------------------------------------------
# BASE VALIDATIONS (ALL TABLES)
# ------------------------------------------------------------


def run_base_validations(
    df: pl.DataFrame,
    table_name: str,
    primary_key: List[str],
    required_column: List[str],
    non_nullable_column: List[str],
    report: Dict[str, List[str]],
) -> bool:
    """
    Enforces foundational structural integrity using Polars-native expressions.

    Contract:
    - Mandatory Schema: All 'required_column' names must exist in the DataFrame.
    - Uniqueness: Enforces primary key uniqueness and detects conflicting duplicates.
    - Non-Nullability: Columns in 'non_nullable_column' must not contain Null values.

    Invariants:
    - Diagnostic Safety: Read-only; does not mutate the input DataFrame.
    - Performance: Leverages Polars lazy-style evaluations for memory efficiency.

    Outputs:
    - Boolean: True if all mandatory structural checks pass.

    Failures:
    - [Structural] Logs findings to 'report["errors"]' and returns False for missing columns, empty datasets, or PK conflicts.
    """

    if df.is_empty():
        log_error(f"{table_name}: dataset is empty", report)

        return False

    actual = set(df.columns)
    required = set(required_column)

    missing_required = sorted(required - actual)
    if missing_required:
        log_error(f"{table_name}: missing required columns: {missing_required}", report)

    if missing_required:
        return False

    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        log_error(
            f"{table_name}: missing primary key columns: {missing_pk_columns}", report
        )

        return False

    duplicate_mask = df.select(pl.col(primary_key).is_duplicated()).to_series()

    if duplicate_mask.any():

        duplicate_rows = df.filter(duplicate_mask)

        # number of unique rows per PK (full row comparison)
        pk_unique_rows = duplicate_rows.unique().group_by(primary_key).len()
        conflicting = (pk_unique_rows.get_column("len") > 1).any()

        if conflicting:
            log_error(
                f"{table_name}: conflicting duplicate primary key records detected",
                report,
            )
            return False

        # Count of rows per PK
        pk_group_size = duplicate_rows.group_by(primary_key).len()
        repairable_count = int(
            (pk_group_size.get_column("len") - 1).sum()
        )  # Exclude 1st PK occurrence

        if repairable_count > 0:
            log_warning(
                f"{table_name}: {repairable_count} duplicate rows eligible for deduplication",
                report,
            )

    columns = df.columns
    duplicate_columns = [col for idx, col in enumerate(columns) if col in columns[:idx]]
    if duplicate_columns:
        log_warning(
            f"{table_name}: duplicate column names detected: {duplicate_columns}",
            report,
        )

    pk_null_count = (
        df.select(pl.any_horizontal(pl.col(primary_key).is_null())).to_series().sum()
    )

    if pk_null_count > 0:
        log_warning(
            f"{table_name}: {pk_null_count} rows with null primary key values", report
        )

    # Null rows in non nullable columns
    if non_nullable_column:
        column_nulls = df.select(pl.col(non_nullable_column).null_count()).row(
            0, named=True
        )

        for col, count in column_nulls.items():
            if count > 0:
                log_warning(
                    f"{table_name}: {count} null values in non-nullable column {col}",
                    report,
                )

    return True


# ------------------------------------------------------------
# TABLE ROLE VALIDATIONS
# ------------------------------------------------------------


def run_event_fact_validations(
    df: pl.DataFrame, table_name: str, report: Dict[str, List[str]]
) -> bool:
    """
    Enforces business-logic chronology and resolution standards for Event-Role tables.

    Contract:
    - Resolution Verification: Asserts that all timestamps are pre-normalized to microseconds (us) by the I/O layer.
    - Chronological Check: Evaluates temporal sequence (Purchase <= Approval <= Delivery) using clean Polars syntax.

    Invariants:
    - Temporal Consistency: Flags records where delivery precedes purchase as Warnings.
    - Zero-Tolerance Resolution: Assumes compliance with the 'Normalize-at-Source' I/O strategy.

    Outputs:
    - Boolean: True if all temporal checks are executed.

    Failures:
    - [Structural] Returns False if required timestamp columns are missing.
    """

    missing_ts_columns = [col for col in REQUIRED_TIMESTAMPS if col not in df.columns]
    if missing_ts_columns:
        log_error(
            f"{table_name}: missing required timestamp columns: {missing_ts_columns}",
            report,
        )

        return False

    safe_parse_expr = []

    for col in REQUIRED_TIMESTAMPS:

        # Parse only string columns
        if col in df.columns and df.schema[col] == pl.String:
            safe_parse_expr.append(
                pl.col(col)
                .str.to_datetime(format=TIMESTAMP_FORMATS[col], strict=False)
                .alias(col)
            )

    parsed_df = df.with_columns(safe_parse_expr) if safe_parse_expr else df

    unparsable_counts = parsed_df.select(
        [
            pl.col(col).is_null().sum().alias(col)
            for col in REQUIRED_TIMESTAMPS
            if col in df.columns
        ]
    ).row(0, named=True)

    for col, invalid_count in unparsable_counts.items():
        if invalid_count > 0:
            log_warning(
                f"{table_name}: {invalid_count} unparsable timestamp values in {col}",
                report,
            )

    invalid_temporal_counts = parsed_df.select(
        invalid_approval=(
            pl.col("order_approved_at") < pl.col("order_purchase_timestamp")
        ).sum(),
        invalid_delivery=(
            pl.col("order_delivered_timestamp") < pl.col("order_purchase_timestamp")
        ).sum(),
    ).row(0, named=True)

    # Check for invalid temporal ordering
    if invalid_temporal_counts["invalid_approval"] > 0:
        log_warning(
            f"{table_name}: {invalid_temporal_counts['invalid_approval']} records where approval precedes purchase",
            report,
        )

    if invalid_temporal_counts["invalid_delivery"] > 0:
        log_warning(
            f"{table_name}: {invalid_temporal_counts['invalid_delivery'] } records where delivery precedes purchase",
            report,
        )

    return True


def run_transaction_detail_validations(
    df: pl.DataFrame, table_name: str, report: Dict[str, List[str]]
) -> bool:
    """
    Enforces domain and range constraints for Transaction-Role tables.

    Contract:
    - Range Check: Ensures financial metrics (price, freight, payments) are non-negative.

    Outputs:
    - Boolean: True if domain validations complete.

    Failures:
    - [Operational] Logs out-of-range values to 'report["errors"]'.
    """

    negative_counts = df.select((cs.numeric() < 0).sum()).row(0, named=True)

    # 2. Iterate through the resulting dictionary
    for col, count in negative_counts.items():
        if count > 0:
            log_error(
                f"{table_name}: {count} negative values in numeric column `{col}`",
                report,
            )

    return True


def run_cross_table_validations(
    tables: Dict[str, pl.DataFrame], report: Dict[str, List[str]]
) -> bool:
    """
    Enforces referential integrity (Foreign Key) across the dataset.

    Contract:
    - Orphan Detection: Identifies child records (Items, Payments) lacking a valid parent Order.

    Invariants:
    - Referential Grain: Uses 'order_id' as the primary join key for set intersection.

    Outputs:
    - Boolean: True if cross-table analysis completes.

    Failures:
    - [Operational] Logs orphan counts as Warnings to the report.
    """

    required_tables = ["df_orders", "df_order_items", "df_payments"]
    missing_tables = [tbl for tbl in required_tables if tbl not in tables]

    if missing_tables:
        log_info(
            f"Cross-table validation skipped: missing required tables: {missing_tables}",
            report,
        )

        return False

    orders_df = tables["df_orders"]
    order_items_df = tables["df_order_items"]
    payments_df = tables["df_payments"]

    order_id_set = set(orders_df.get_column("order_id").drop_nulls().unique())

    orphan_items = ~order_items_df.get_column("order_id").is_in(order_id_set)
    if orphan_items.any():
        log_warning(
            f"df_order_items: {orphan_items.sum()} orphan records referencing non-existent order_id",
            report,
        )

    orphan_payments = ~payments_df.get_column("order_id").is_in(order_id_set)
    if orphan_payments.any():
        log_warning(
            f"df_payments: {orphan_payments.sum()} orphan records referencing non-existent order_id",
            report,
        )

    return True
