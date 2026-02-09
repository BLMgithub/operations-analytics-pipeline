# =============================================================================
# VALIDATE RAW FULFILLMENT DATA
# =============================================================================
# - Enforce structural and semantic integrity of raw fulfillment data
# - Block data that would corrupt downstream joins, aggregations, or timelines
# - Designed for deterministic execution in CI/CD pipelines


import os
import sys
import glob
from typing import Dict, List, Optional
import pandas as pd


# ------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------

RAW_DATA_BASE_PATH = 'data/raw'
VALIDATE_TEST = os.getenv('VALIDATE_TEST', 'false').lower() == 'true'

PARTITIONS = ['train']

if VALIDATE_TEST:
    PARTITIONS.append('test')

TABLE_CONFIG = {
    'df_Orders': {
        'role': 'event_fact',
        'primary_key': ['order_id']
    },
    'df_OrderItems': {
        'role': 'transaction_detail',
        'primary_key': ['order_id']
    },
    'df_Customers': {
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
                         ) -> None:
    """
    Base structural validations.
    
    Stops if structure is broken.
    """
    
    if df.empty:
        log_error(f'{table_name}: dataset is empty', report)

        return

    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    if duplicate_columns:
        log_error(
            f'{table_name}: duplicate column names detected: {duplicate_columns}', 
            report
            )

    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        log_error(
            f'{table_name}: missing primary key column(s): {missing_pk_columns}', 
            report
            )

        return

    pk_null_count = df[primary_key].isnull().any(axis=1).sum()
    if pk_null_count > 0:
        log_error(
            f'{table_name}: {pk_null_count} row(s) with null primary key values', 
            report
            )


    duplicate_pk_count = df.duplicated(subset=primary_key).sum()
    if duplicate_pk_count > 0:
        log_error(
            f'{table_name}: {duplicate_pk_count} duplicated primary key value(s)', 
            report
            )


# ------------------------------------------------------------
# EVENT FACT VALIDATIONS
# ------------------------------------------------------------

def run_event_fact_validations(df: pd.DataFrame,
                               table_name: str,
                               report: Dict[str, List[str]]
                               ) -> None:
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

        return

    # Timestamps completeness
    parsed = {}
    parsing_failed = False

    for col in required_timestamps:
        ts = pd.to_datetime(df[col], errors='coerce')
        parsed[col] = ts

        invalid_count = ts.isna().sum()
        if invalid_count > 0:
            parsing_failed = True
            log_error(
                f'{table_name}: {invalid_count} unparsable timestamp value(s) in `{col}`',
                report
                )

    if parsing_failed:

        return

    purchase_ts = parsed['order_purchase_timestamp']
    approved_ts = parsed['order_approved_at']
    delivered_ts = parsed['order_delivered_timestamp']


    # Approval before Purchase
    invalid_approval = (approved_ts < purchase_ts).sum()
    if invalid_approval > 0:
        log_error(
            f'{table_name}: {invalid_approval} record(s) where approval precedes purchase',
            report
            )

        return

    # Delivery before Purchase
    invalid_delivery = (delivered_ts < purchase_ts).sum()
    if invalid_delivery > 0:
        log_error(
            f'{table_name}: {invalid_delivery} record(s) where delivery precedes purchase',
            report
            )
        
        return


# ------------------------------------------------------------
# TRANSACTION DETAIL VALIDATIONS
# ------------------------------------------------------------ 

def run_transaction_detail_validations(df: pd.DataFrame,
                                       table_name: str,
                                       report: Dict[str, List[str]]
                                       ) -> None:
    """
    Transaction detail validations.

    Stops if aggregations would be corrupted.
    """
    
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()

    for col in numeric_columns:
        negative_count = (df[col] < 0).sum()
        if negative_count > 0:
            log_error(
                f'{table_name}: {negative_count} negative value(s) in numeric column `{col}`', 
                report
                )

            return
    

# ------------------------------------------------------------
# CROSS-TABLE VALIDATIONS
# ------------------------------------------------------------

def run_cross_table_validations(tables: Dict[str, pd.DataFrame],
                                report: Dict[str, List[str]]
                                ) -> None:
    """
    Cross-table validations.

    Stops if parent-child attachment semantics are broken.
    """

    required_tables = ['df_Orders', 'df_OrderItems', 'df_payments']
    missing_tables = [t for t in required_tables if t not in tables]

    if missing_tables:
        log_error(
            f'Cross-table validation failed: missing required table(s): {missing_tables}', 
            report
            )
        
        return

    orders_df = tables['df_Orders']
    order_items_df = tables['df_OrderItems']
    payments_df = tables['df_payments']

    # Orders PK reference
    order_id_set = set(orders_df['order_id'].dropna().unique())

    # OrderItems to Orders integrity
    orphan_items = ~order_items_df['order_id'].isin(order_id_set)
    if orphan_items.any():
        log_error(
            f'df_OrderItems: {orphan_items.sum()} orphan record(s) referencing non-existent order_id', 
            report
            )
        
        return

    # Payments to Orders integrity
    orphan_payments = ~payments_df['order_id'].isin(order_id_set)
    if orphan_payments.any():
        log_error(
            f'df_payments: {orphan_payments.sum()} orphan record(s) referencing non-existent order_id', 
            report
            )

        return

# ------------------------------------------------------------
# INPUT-OUTPUT HELPERS
# ------------------------------------------------------------

def load_csv_file(csv_path: str, table_name: str, 
                  report: Dict[str, List[str]]
                  ) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(csv_path)
        log_info(f'Loaded {table_name} file: {os.path.basename(csv_path)} ({len(df)} rows)', report)

        return df
    
    except Exception as e:
        log_error(f'Failed to load {table_name} file {csv_path}: {e}', report)

        return None


def load_logical_table(partition_path: str,
                       table_name: str,
                       report: Dict[str, List[str]]
                       ) -> Optional[pd.DataFrame]:
    
    """
    Load and concatenate all CSV files belonging to a logical table.
    Files are identified by filename prefix: <table_name>*.csv
    """

    pattern = os.path.join(partition_path, f'{table_name}*.csv')
    csv_files = glob.glob(pattern)

    if not csv_files:
        log_error(f'{table_name}: no files found matching pattern {pattern}', report)
        
        return None

    dfs = []
    for csv_path in csv_files:
        df = load_csv_file(csv_path, table_name, report)
        if df is not None:
            dfs.append(df)

    if not dfs:
        log_error(f'{table_name}: all matching files failed to load', report)

        return None

    combined_df = pd.concat(dfs, ignore_index=True)
    log_info(f'{table_name}: combined {len(csv_files)} file(s) into '
             f'{len(combined_df)} rows',
             report)

    return combined_df


# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------

def main() -> None:
    report = init_report()

    for partition in PARTITIONS:
        partition_path = os.path.join(RAW_DATA_BASE_PATH, partition)
        tables: Dict[str, pd.DataFrame] = {}

        for table_name, config in TABLE_CONFIG.items():
            csv_path = os.path.join(partition_path, f'{table_name}.csv')

            if not os.path.exists(csv_path):
                log_error(
                    f'Missing file: {csv_path}', report)

                continue

            df = load_logical_table(partition_path, table_name, report)
            if df is None:
                
                continue

            run_base_validations(df, table_name, config['primary_key'], report)

            if config['role'] == 'event_fact':
                run_event_fact_validations(df, table_name, report)

            elif config['role'] == 'transaction_detail':
                run_transaction_detail_validations(df, table_name, report)

            tables[table_name] = df

        run_cross_table_validations(tables, report)

    if report['errors']:
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()


# =============================================================================
# END OF SCRIPT
# =============================================================================