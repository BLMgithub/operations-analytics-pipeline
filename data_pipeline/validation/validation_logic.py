# =============================================================================
# Validation Stage Logic
# =============================================================================

from typing import Dict, List
import pandas as pd
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
    df: pd.DataFrame,
    table_name: str,
    primary_key: List[str],
    required_column: List[str],
    non_nullable_column: List[str],
    report: Dict[str, List[str]],
) -> bool:
    """
    Enforces foundational structural integrity for a logical table.

    Contract:
    - Mandatory Schema: All 'required_column' names must exist in the DataFrame.
    - Uniqueness: Enforces primary key uniqueness and detects conflicting duplicates.
    - Non-Nullability: Columns in 'non_nullable_column' must not contain NaN values.

    Invariants:
    - Diagnostic Safety: Read-only; does not mutate the input DataFrame.

    Outputs:
    - Boolean: True if all mandatory structural checks pass.

    Failures:
    - [Structural] Logs findings to 'report["errors"]' and returns False for missing columns, empty datasets, or PK conflicts.
    """

    if df.empty:
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

    duplicate_mask = df.duplicated(subset=primary_key, keep=False)
    if duplicate_mask.any():

        duplicate_rows = df[duplicate_mask]

        # Count of rows per PK
        pk_group_size = duplicate_rows.groupby(primary_key, dropna=False).size()

        # number of unique rows per PK (full row comparison)
        pk_unique_rows = (
            duplicate_rows.drop_duplicates().groupby(primary_key, dropna=False).size()
        )

        conflicting = (pk_unique_rows > 1).any()

        if conflicting:
            log_error(
                f"{table_name}: conflicting duplicate primary key records detected",
                report,
            )
            return False

        repairable_count = int((pk_group_size - 1).sum())  # Exclude 1st PK occurrence

        if repairable_count > 0:
            log_warning(
                f"{table_name}: {repairable_count} duplicate rows eligible for deduplication",
                report,
            )

    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    if duplicate_columns:
        log_warning(
            f"{table_name}: duplicate column names detected: {duplicate_columns}",
            report,
        )

    pk_null_count = df[primary_key].isnull().any(axis=1).sum()
    if pk_null_count > 0:
        log_warning(
            f"{table_name}: {pk_null_count} rows with null primary key values", report
        )

    # Null rows in non nullable columns
    column_nulls = df[non_nullable_column].isna().sum()

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
    df: pd.DataFrame, table_name: str, report: Dict[str, List[str]]
) -> bool:
    """
    Enforces business-logic chronology for Event-Role tables.

    Contract:
    - Chronological Check: Evaluates temporal sequence (Purchase <= Approval <= Delivery).
    - Parseability: Validates timestamp string compatibility with system formats.

    Invariants:
    - Temporal Consistency: Flags records where delivery precedes purchase as Warnings.

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

    parsed = {}

    for col in REQUIRED_TIMESTAMPS:
        ts = pd.to_datetime(
            df[col],
            format=TIMESTAMP_FORMATS[col],
            errors="coerce",
        )
        parsed[col] = ts

        invalid_count = ts.isna().sum()
        if invalid_count > 0:
            log_warning(
                f"{table_name}: {invalid_count} unparsable timestamp values in {col}",
                report,
            )

    purchase_ts = parsed["order_purchase_timestamp"]
    approved_ts = parsed["order_approved_at"]
    delivered_ts = parsed["order_delivered_timestamp"]

    # Check for invalid temporal ordering
    invalid_approval = (approved_ts < purchase_ts).sum()
    if invalid_approval > 0:
        log_warning(
            f"{table_name}: {invalid_approval} records where approval precedes purchase",
            report,
        )

    invalid_delivery = (delivered_ts < purchase_ts).sum()
    if invalid_delivery > 0:
        log_warning(
            f"{table_name}: {invalid_delivery} records where delivery precedes purchase",
            report,
        )

    return True


def run_transaction_detail_validations(
    df: pd.DataFrame, table_name: str, report: Dict[str, List[str]]
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

    numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()

    for col in numeric_columns:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            log_error(
                f"{table_name}: {negative_count} negative values in numeric column `{col}`",
                report,
            )

    return True


def run_cross_table_validations(
    tables: Dict[str, pd.DataFrame], report: Dict[str, List[str]]
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

    # Orders PK reference
    order_id_set = set(orders_df["order_id"].dropna().unique())

    # OrderItems to Orders integrity
    orphan_items = ~order_items_df["order_id"].isin(order_id_set)
    if orphan_items.any():
        log_warning(
            f"df_order_items: {orphan_items.sum()} orphan records referencing non-existent order_id",
            report,
        )

    # Payments to Orders integrity
    orphan_payments = ~payments_df["order_id"].isin(order_id_set)
    if orphan_payments.any():
        log_warning(
            f"df_payments: {orphan_payments.sum()} orphan records referencing non-existent order_id",
            report,
        )

    return True
