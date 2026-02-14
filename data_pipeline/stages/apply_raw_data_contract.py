# =============================================================================
# Raw Data Structural Contract Enforcement
# =============================================================================
# - Enforce non-negotiable structural contracts on raw event data
# - Remove records that violate declared schema, key, or temporal invariants
# - Produce a contract-compliant dataset suitable for CI validation and downstream assembly


import pandas as pd
from typing import List
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file
from data_pipeline.shared.run_context import RunContext
from pathlib import Path

# ------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------

TABLE_CONFIG = {
    'df_orders': {
        'role': 'event_fact',
        'primary_key': ['order_id']
    },
    'df_order_items': {
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
# FATAL VALIDATION
# ------------------------------------------------------------

def validate_primary_key(df: pd.DataFrame, primary_key: list[str] )-> bool:
    """
    Primary key must be present and unique.
    
    Any violation halts contract enforcement.
    """

    missing_pk_columns = [col for col in primary_key if col not in df.columns]
    if missing_pk_columns:

        return False

    duplicated_pk_count = df.duplicated(subset= primary_key).sum()
    if duplicated_pk_count > 0:
        
        return False

    return True


def validate_required_event_timestamps(df: pd.DataFrame) -> bool:
    """
    Required event timestamps must be present.

    Violation halts contract enforcement.
    """

    missing_ts_columns = [col for col in REQUIRED_TIMESTAMPS if col not in df.columns]
    if missing_ts_columns:

        return False
    
    return True
    

# ------------------------------------------------------------
# CONTRACT ENFORCEMENT
# ------------------------------------------------------------

def deduplicate_exact_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove exact duplicate rows representing the same event.
    """

    initial_count = df.shape[0]
    duplicated_mask = df.duplicated()

    if duplicated_mask.any():

        df = df.drop_duplicates()
        removed_count = initial_count - df.shape[0]
        
    else:
        removed_count = 0

    return df, removed_count


def remove_unparsable_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove rows where required timestamps cannot be parsed.
    """

    initial_count = df.shape[0]
    unparsable_mask = pd.Series(False, index=df.index)

    for col in REQUIRED_TIMESTAMPS:
        ts = pd.to_datetime(df[col], errors="coerce")

        # accumulate True for every NaT
        unparsable_mask |= ts.isna()

    if unparsable_mask.any():

        df = df[~unparsable_mask]
        remove_count =  initial_count - df.shape[0]
        
    else:
        remove_count = 0

    return df, remove_count


def remove_impossible_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove rows violating declared temporal invariants (e.g. delivery_date < order_date)
    """

    purchase_ts = pd.to_datetime(df['order_purchase_timestamp'])
    approved_ts = pd.to_datetime(df['order_approved_at'])
    delivered_ts = pd.to_datetime(df['order_delivered_timestamp'])

    invalid_mask = ((approved_ts < purchase_ts) | (delivered_ts < purchase_ts))
    initial_count = df.shape[0]

    if invalid_mask.any():

        df = df[~invalid_mask]
        remove_count = initial_count - df.shape[0]
        
    else:
        remove_count = 0

    return df, remove_count


# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------

def apply_contract(run_context: RunContext, table_name: str) -> dict:
    
    report = {
    'table': table_name,
    'initial_rows': 0,
    'final_rows': 0,
    'deduplicated_rows': 0,
    'removed_unparsable_timestamps': 0,
    'removed_impossible_timestamps': 0,
    'status': 'success',
    'errors': [] 
    }

    if table_name not in TABLE_CONFIG:
        report['status'] = 'failed'
        report['errors'].append(f'Unknown table: {table_name}')
        return report

    base_path = run_context.raw_snapshot_path
    config = TABLE_CONFIG[table_name]

    df = load_logical_table(base_path, table_name)
   
    if df is None:
        report['status'] = 'failed'
        report['errors'].append('Failed to load logical table')
        return report
     
    report['initial_rows'] = len(df)
    
    if not validate_primary_key(df, config['primary_key']):
        report['status'] = 'failed'
        report['errors'].append('Primary key violation detected')
        return report

    if config['role'] == 'event_fact':
        
        if not validate_required_event_timestamps(df):
            report['status'] = 'failed'
            report['errors'].append('Missing required timestamp(s)')
            return report

        df, removed = deduplicate_exact_events(df)
        report['deduplicated_rows'] += removed
        
        df, removed = remove_unparsable_timestamps(df)
        report['removed_unparsable_timestamps'] += removed
        
        df, removed = remove_impossible_timestamps(df)
        report['removed_impossible_timestamps'] += removed

    elif config['role'] == 'transaction_detail':
        df, removed = deduplicate_exact_events(df)
        report['deduplicated_rows'] += removed
    
    elif config['role'] == 'entity_reference':
        pass

    report['final_rows'] = len(df)
    
    output_path = run_context.contracted_path / f'{table_name}_contracted.parquet'
    if not export_file(df, output_path):
        report['status'] = 'failed'
        report['errors'].append('Export failed')
        
    return report

# =============================================================================
# END OF SCRIPT
# =============================================================================