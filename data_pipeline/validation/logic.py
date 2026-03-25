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
    Baseline structural gate for all tables.

    Collects data quality findings and classifies severity:
    - `errors` - dataset is structurally invalid; downstream validations should stop
    - `warnings` - admissible data quality issues that may be repairable

    errors:
    - dataset is empty
    - missing required column(s)
    - missing primary key column(s)
    - conflicting duplicate primary keys

    warnings:
    - null rows in non-nullable column(s)
    - duplicate columns
    - null primary key values
    - identical duplicates
    """

    if df.empty:
        log_error(f"{table_name}: dataset is empty", report)

        return False

    actual = set(df.columns)
    required = set(required_column)

    missing_required = sorted(required - actual)
    if missing_required:
        log_error(
            f"{table_name}: missing required column(s): {missing_required}", report
        )

    if missing_required:
        return False

    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        log_error(
            f"{table_name}: missing primary key column(s): {missing_pk_columns}", report
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
                f"{table_name}: {repairable_count} duplicate row(s) eligible for deduplication",
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
            f"{table_name}: {pk_null_count} row(s) with null primary key values", report
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
# EVENT FACT VALIDATIONS
# ------------------------------------------------------------


def run_event_fact_validations(
    df: pd.DataFrame, table_name: str, report: Dict[str, List[str]]
) -> bool:
    """
    Event fact validation layer.

    Collects data quality findings and classifies severity:
    - `errors` - dataset is structurally invalid; downstream validations should stop
    - `warnings` - admissible data quality issues that may be repairable

    errors:
    - missing required timestamp column(s)

    warnings:
    - unparsable timestamp values in required timestamp fields
    - approval timestamp earlier than purchase timestamp
    - delivery timestamp earlier than purchase timestamp
    """

    missing_ts_columns = [col for col in REQUIRED_TIMESTAMPS if col not in df.columns]
    if missing_ts_columns:
        log_error(
            f"{table_name}: missing required timestamp column(s): {missing_ts_columns}",
            report,
        )

        return False

    parsed = {}

    # Required timestamps column and format
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
                f"{table_name}: {invalid_count} unparsable timestamp value(s) in {col}",
                report,
            )

    purchase_ts = parsed["order_purchase_timestamp"]
    approved_ts = parsed["order_approved_at"]
    delivered_ts = parsed["order_delivered_timestamp"]

    # Check for invalid temporal ordering such as:
    # Approval before Purchase or Delivery before Purchase
    invalid_approval = (approved_ts < purchase_ts).sum()
    if invalid_approval > 0:
        log_warning(
            f"{table_name}: {invalid_approval} record(s) where approval precedes purchase",
            report,
        )

    invalid_delivery = (delivered_ts < purchase_ts).sum()
    if invalid_delivery > 0:
        log_warning(
            f"{table_name}: {invalid_delivery} record(s) where delivery precedes purchase",
            report,
        )

    return True


# ------------------------------------------------------------
# TRANSACTION DETAIL VALIDATIONS
# ------------------------------------------------------------


def run_transaction_detail_validations(
    df: pd.DataFrame, table_name: str, report: Dict[str, List[str]]
) -> bool:
    """
    Transaction detail validation.

    Collects error-level data quality findings.

    errors:
    - negative values in numeric columns
    """

    numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()

    for col in numeric_columns:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            log_error(
                f"{table_name}: {negative_count} negative value(s) in numeric column `{col}`",
                report,
            )

    return True


# ------------------------------------------------------------
# CROSS-TABLE VALIDATIONS
# ------------------------------------------------------------


def run_cross_table_validations(
    tables: Dict[str, pd.DataFrame], report: Dict[str, List[str]]
) -> bool:
    """
    Cross-table integrity validation.

    Collects data quality findings and classifies severity:
    - `warnings` - admissible referential integrity issues
    - `info` - validation skipped due to missing required tables

    info:
    - validation skipped when required tables are unavailable

    warnings:
    - order items referencing non-existent order_id
    - payments referencing non-existent order_id
    """

    required_tables = ["df_orders", "df_order_items", "df_payments"]
    missing_tables = [tbl for tbl in required_tables if tbl not in tables]

    if missing_tables:
        log_info(
            f"Cross-table validation skipped: missing required table(s): {missing_tables}",
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
            f"df_order_items: {orphan_items.sum()} orphan record(s) referencing non-existent order_id",
            report,
        )

    # Payments to Orders integrity
    orphan_payments = ~payments_df["order_id"].isin(order_id_set)
    if orphan_payments.any():
        log_warning(
            f"df_payments: {orphan_payments.sum()} orphan record(s) referencing non-existent order_id",
            report,
        )

    return True
