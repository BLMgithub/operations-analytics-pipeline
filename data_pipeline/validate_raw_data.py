# =============================================================================
# VALIDATE RAW FULFILLMENT DATA
# =============================================================================
# - Enforce structural and semantic integrity of raw fulfillment data
# - Block data that would corrupt downstream joins, aggregations, or timelines
# - Designed for deterministic execution in CI/CD pipelines


import os
import sys
from typing import Dict, List
import pandas as pd
from .io.raw_loader_exporter import load_logical_table


# ------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------

RAW_DATA_BASE_PATH = 'data/raw'
VALIDATE_TEST = os.getenv('VALIDATE_TEST', 'false').lower() == 'true'

PARTITIONS = ['train']

if VALIDATE_TEST:
    PARTITIONS.append('test')

TABLE_CONFIG = {
    'df_orders': {
        'role': 'event_fact',
        'primary_key': ['order_id']
    },
    'df_orderItems': {
        'role': 'transaction_detail',
        'primary_key': ['order_id']
    },
    'df_customers': {
        'role': 'entity_reference',
        'primary_key': ['customer_id']
    },
    'df_payments': {
        'role': 'transaction_detail',
        'primary_key': ['order_id', 'payment_sequential']
    },
    'df_products': {
        'role': 'entity_reference',
        'primary_key': ['product_id']
    },
}


# ------------------------------------------------------------
# VALIDATION REPORT & LOGS
# ------------------------------------------------------------

def init_report() -> Dict[str, List[str]]:

    return {
        'errors': [],
        'warnings': [],
        'info': []
    }


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[INFO] {message}')
    report['info'].append(message)


def log_warning(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[WARNING] {message}')
    report['warnings'].append(message)


def log_error(message: str, report: Dict[str, List[str]]) -> None:
    print(f'[ERROR] {message}')
    report['errors'].append(message)


# ------------------------------------------------------------
# BASE VALIDATIONS (ALL TABLES)
# ------------------------------------------------------------

def run_base_validations(df: pd.DataFrame,
                         table_name: str,
                         primary_key: List[str],
                         report: Dict[str, List[str]]
                         ) -> bool:
    """
    Base structural validations.
    
    Stops if structure is broken.
    """
    
    if df.empty:
        log_error(f'{table_name}: dataset is empty', report)

        return False
    
    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        log_error(
            f'{table_name}: missing primary key column(s): {missing_pk_columns}', 
            report
            )

        return False
    
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    if duplicate_columns:
        log_warning(
            f'{table_name}: duplicate column names detected: {duplicate_columns}', 
            report
            )

    pk_null_count = df[primary_key].isnull().any(axis=1).sum()
    if pk_null_count > 0:
        log_warning(
            f'{table_name}: {pk_null_count} row(s) with null primary key values', 
            report
            )

    duplicate_pk_count = df.duplicated(subset=primary_key).sum()
    if duplicate_pk_count > 0:
        log_warning(
            f'{table_name}: {duplicate_pk_count} duplicated primary key value(s)', 
            report
            )

    return True


# ------------------------------------------------------------
# EVENT FACT VALIDATIONS
# ------------------------------------------------------------

def run_event_fact_validations(df: pd.DataFrame,
                               table_name: str,
                               report: Dict[str, List[str]]
                               ) -> bool:
    """
    Event fact validations.

    Stops if timeline integrity is broken.
    """

    required_timestamps = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_timestamp',
        'order_estimated_delivery_date',
    ]

    missing_ts_columns = [c for c in required_timestamps if c not in df.columns]
    if missing_ts_columns:
        log_error(
            f'{table_name}: missing required timestamp column(s): {missing_ts_columns}', 
            report
            )

        return False

    # Timestamps completeness
    parsed = {}

    for col in required_timestamps:
        ts = pd.to_datetime(df[col], errors='coerce')
        parsed[col] = ts

        invalid_count = ts.isna().sum()
        if invalid_count > 0:
            log_warning(
                f'{table_name}: {invalid_count} unparsable timestamp value(s) in `{col}`',
                report
                )

    purchase_ts = parsed['order_purchase_timestamp']
    approved_ts = parsed['order_approved_at']
    delivered_ts = parsed['order_delivered_timestamp']

    # Approval before Purchase
    invalid_approval = (approved_ts < purchase_ts).sum()
    if invalid_approval > 0:
        log_warning(
            f'{table_name}: {invalid_approval} record(s) where approval precedes purchase',
            report
            )

    # Delivery before Purchase
    invalid_delivery = (delivered_ts < purchase_ts).sum()
    if invalid_delivery > 0:
        log_warning(
            f'{table_name}: {invalid_delivery} record(s) where delivery precedes purchase',
            report
            )
    
    return True


# ------------------------------------------------------------
# TRANSACTION DETAIL VALIDATIONS
# ------------------------------------------------------------ 

def run_transaction_detail_validations(df: pd.DataFrame,
                                       table_name: str,
                                       report: Dict[str, List[str]]
                                       ) -> bool:
    """
    Transaction detail validations.

    Stops if aggregations would be corrupted.
    """
    
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()

    for col in numeric_columns:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            log_warning(
                f'{table_name}: {negative_count} negative value(s) in numeric column `{col}`', 
                report
                )
    
    return True

# ------------------------------------------------------------
# CROSS-TABLE VALIDATIONS
# ------------------------------------------------------------

def run_cross_table_validations(tables: Dict[str, pd.DataFrame],
                                report: Dict[str, List[str]]
                                ) -> bool:
    """
    Cross-table validations.

    Stops if parent-child attachment semantics are broken.
    """

    required_tables = ['df_orders', 'df_orderItems', 'df_payments']
    missing_tables = [t for t in required_tables if t not in tables]

    if missing_tables:
        log_error(
            f'Cross-table validation failed: missing required table(s): {missing_tables}', 
            report
            )
        
        return False

    orders_df = tables['df_orders']
    order_items_df = tables['df_orderItems']
    payments_df = tables['df_payments']

    # Orders PK reference
    order_id_set = set(orders_df['order_id'].dropna().unique())

    # OrderItems to Orders integrity
    orphan_items = ~order_items_df['order_id'].isin(order_id_set)
    if orphan_items.any():
        log_warning(
            f'df_orderItems: {orphan_items.sum()} orphan record(s) referencing non-existent order_id', 
            report
            )

    # Payments to Orders integrity
    orphan_payments = ~payments_df['order_id'].isin(order_id_set)
    if orphan_payments.any():
        log_warning(
            f'df_payments: {orphan_payments.sum()} orphan record(s) referencing non-existent order_id', 
            report
            )
    
    return True


# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------

def main() -> None:

    report = init_report()

    def info(msg: str):
        log_info(msg, report)

    def error(msg: str):
        log_error(msg, report)

    for partition in PARTITIONS:
        partition_path = os.path.join(RAW_DATA_BASE_PATH, partition)
        tables: Dict[str, pd.DataFrame] = {}

        for table_name, config in TABLE_CONFIG.items():
            csv_path = os.path.join(partition_path, f'{table_name}.csv')

            if not os.path.exists(csv_path):
                log_error(f'Missing file: {csv_path}', report)

                continue

            df = load_logical_table(partition_path, table_name, log_info = info, log_error = error)
            if df is None:

                continue

            if not run_base_validations(df, table_name, config['primary_key'], report):
                
                continue

            if config['role'] == 'event_fact':
                run_event_fact_validations(df, table_name, report)

            elif config['role'] == 'transaction_detail':
                run_transaction_detail_validations(df, table_name, report)

            tables[table_name] = df

        run_cross_table_validations(tables, report)

    if report['errors'] or report['warnings']:
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()


# =============================================================================
# END OF SCRIPT
# =============================================================================