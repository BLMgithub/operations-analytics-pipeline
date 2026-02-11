# =============================================================================
# Raw Data Structural Contract Enforcement
# =============================================================================
# - Enforce non-negotiable structural contracts on raw event data
# - Remove records that violate declared schema, key, or temporal invariants
# - Produce a contract-compliant dataset suitable for CI validation and downstream assembly


import os
import sys
import argparse
import logging
import pandas as pd
from typing import Dict, List
from .io.raw_loader_exporter import load_logical_table, export_file

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

REQUIRED_TIMESTAMPS = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_timestamp',
    'order_estimated_delivery_date',
]

# ------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s | %(levelname)s | %(message)s ',
    datefmt= '%Y-%m-%d %H:%M:%S',
    stream = sys.stdout
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# FATAL VALIDATION
# ------------------------------------------------------------

def validate_primary_key(df: pd.DataFrame, primary_key:str )-> None:
    """
    Primary key must be present and unique.
    
    Any violation halts contract enforcement.
    """

    logger.info('Validating primary key uniqueness and completeness')

    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:
        logger.error(
            f'Missing primary key column(s): {missing_pk_columns}'
            )

        raise RuntimeError('Validation failed violation detected!')

    duplicated_pk_count = df.duplicated(subset= primary_key).sum()
    if duplicated_pk_count > 0:
        logger.error(
            f'Duplicated primary key value(s): {duplicated_pk_count}'
            )

        raise RuntimeError('Validation failed violation detected!')

    logger.info('Primary key validation passed')

def validate_required_event_timestamps(df: pd.DataFrame) -> None:
    """
    Required event timestamps must be present.

    Violation halts contract enforcement.
    """

    logger.info('Validating required event timestamps')

    missing_ts_columns = [col for col in REQUIRED_TIMESTAMPS if col not in df.columns]
    if missing_ts_columns:
        logger.error(
            f'Missing required event timestamps {missing_ts_columns}'
            )

        raise RuntimeError('Validation failed violation detected!')
    
    logger.info('Required timestamps validation passed')
    

# ------------------------------------------------------------
# CONTRACT ENFORCEMENT
# ------------------------------------------------------------

def deduplicate_exact_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove exact duplicate rows representing the same event.
    """

    logger.info(f'Enforcing deduplication contract!')

    initial_count = df.shape[0]
    duplicated_mask = df.duplicated()

    if duplicated_mask.any():
        logger.info(
            f'Detected duplication! Initial rows: {initial_count}'
            )

        df = df.drop_duplicates()

        result_count = df.shape[0]
        logger.info(
            f'Deduplication completed! Resulting rows: {result_count}'
            )
        
    else:
        logger.info('Contract deduplication passed!')

    return df


def remove_unparsable_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where required timestamps cannot be parsed.
    """

    logger.info(f'Enforcing unparsable timestamps contract!')

    initial_count = df.shape[0]
    unparsable_mask = pd.Series(False, index=df.index)

    for col in REQUIRED_TIMESTAMPS:
        ts = pd.to_datetime(df[col], errors="coerce")

        # accumulate True for every NaT
        unparsable_mask |= ts.isna()

    if unparsable_mask.any():
        logger.info(
            f'Detected unparsable timestamps! Initial rows: {initial_count}'
            )

        df = df[~unparsable_mask]

        result_count = df.shape[0]
        logger.info(
            f'Unparsable timestamps removal completed! Result rows: {result_count}'
            )
        
    else:
        logger.info(f'Contract unparsable timestamps passed!')

    return df


def remove_impossible_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows violating declared temporal invariants (e.g. delivery_date < order_date)
    """

    logger.info('Enfocring impossible timestamps contract!')

    purchase_ts = pd.to_datetime(df['order_purchase_timestamp'])
    approved_ts = pd.to_datetime(df['order_approved_at'])
    delivered_ts = pd.to_datetime(df['order_delivered_timestamp'])

    invalid_mask = ((approved_ts < purchase_ts) | (delivered_ts < purchase_ts))
    initial_count = df.shape[0]

    if invalid_mask.any():
        logger.info(
            f'Detected impossible timestamps! Initial rows: {initial_count}'
            )

        df = df[~invalid_mask]

        result_count = df.shape[0]
        logger.info(
            f'Impossible timestamp removal completed! Result rows: {result_count}'
        )

    else:
        logger.info(f'Contract impossible timestamps passed!')

    return df

# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------

def apply_contract(table_name: str, partition: str) -> None:

    partition_path = os.path.join(RAW_DATA_BASE_PATH, partition)

    if table_name not in TABLE_CONFIG:
        raise ValueError(f'Unknown table: {table_name}')

    config = TABLE_CONFIG[table_name]

    df = load_logical_table(partition_path, table_name)

    if df is None:
        raise RuntimeError('Failed to load logical table')

    validate_primary_key(df, config['primary_key'])
    deduplicate_exact_events(df)

    if config['role'] == 'event_fact':
        validate_required_event_timestamps(df)

        df = remove_unparsable_timestamps(df)
        df = remove_impossible_timestamps(df)

    output_path = os.path.join(
        'data/contracted', 
        partition, 
        f'{table_name}.csv'
        )

    export_file(df, output_path)


def main():

    parser = argparse.ArgumentParser(
        description = 'Apply structural contract to a specific raw table.'
    )

    parser.add_argument( '--table',
                        type= str,
                        required= True,
                        help= 'table name (e.g. df_Orders)'
                        )

    parser.add_argument( '--partition',
                        type = str,
                        required = True,
                        help = 'Partition name (e.g. train)'
                        )

    args = parser.parse_args()

    apply_contract(table_name = args.table.strip().lower(), partition = args.partition)


if __name__ == '__main__':
    main()


# =============================================================================
# END OF SCRIPT
# =============================================================================