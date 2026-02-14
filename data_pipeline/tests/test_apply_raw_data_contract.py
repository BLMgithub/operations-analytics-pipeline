# =============================================================================
# UNIT TESTS FOR apply_raw_data_contract.py
# =============================================================================

import pandas as pd
import pytest
from shutil import copytree

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.apply_raw_data_contract import(
    validate_primary_key,
    validate_required_event_timestamps,
    deduplicate_exact_events,
    remove_unparsable_timestamps,
    remove_impossible_timestamps,
    apply_contract
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------

@pytest.fixture
def valid_orders_df():
    return pd.DataFrame({
        'order_id': ['o1', 'o2'],
        'order_purchase_timestamp': ['2023-01-01', '2023-01-02'],
        'order_approved_at': ['2023-01-01', '2023-01-02'],
        'order_delivered_timestamp': ['2023-01-03', '2023-01-04'],
        'order_estimated_delivery_date': ['2023-01-05', '2023-01-06'],
    })

@pytest.fixture
def invalid_order_df():
    return pd.DataFrame({
        'order_id': ['o1', 'o2'],
        'order_purchase_timestamp': ['2023-01-02', 'invalid'],
        'order_approved_at': ['2023-01-01', 'invalid'],
        'order_delivered_timestamp': ['2023-01-03', '2023-01-04'],
        'order_estimated_delivery_date': ['2023-01-05', '2023-01-06']
    })

@pytest.fixture
def invalid_temporal_order_df():
    return pd.DataFrame({
        'order_id': ['o1', 'o2'],
        'order_purchase_timestamp': ['2023-01-01', '2023-01-02'],
        'order_approved_at': ['2022-01-01', '2023-01-02'],
        'order_delivered_timestamp': ['2023-01-03', '2023-01-04'],
        'order_estimated_delivery_date': ['2023-01-05', '2023-01-06']
    })

# ------------------------------------------------------------
# FATAL VALIDATION
# ------------------------------------------------------------

def test_validate_primary_key_passes(valid_orders_df):
    ok = validate_primary_key(valid_orders_df, ['order_id'])

    assert ok == True


def test_validate_primary_key_fails_on_missing_pk():
    df = pd.DataFrame({'x': [1,2]})
    ok = validate_primary_key(df, ['id'])

    assert ok == False


def test_validate_primary_key_fails_on_duplicated_pk():
    df = pd.DataFrame({
        'id': ['x','x'],
        'value': [1,2]
    })

    ok = validate_primary_key(df, ['id'])

    assert ok == False


def test_validate_required_event_timestamps_passes(valid_orders_df):
    ok =  validate_required_event_timestamps(valid_orders_df)

    assert ok == True


def test_validate_required_event_timestamps_fails_on_missing_timestamp(valid_orders_df):
    df = valid_orders_df.drop(columns= 'order_delivered_timestamp')

    ok = validate_required_event_timestamps(df)

    assert ok == False

# ------------------------------------------------------------
# DEDUPLICATION CONTRACT
# ------------------------------------------------------------

def test_deduplicate_exact_events_passed():
    df = pd.DataFrame({
        'id': ['x', 'x'],
        'value': [1, 2]
    })

    result, _ = deduplicate_exact_events(df)

    assert len(result) == 2
    assert result.iloc[0]['id'] == 'x'
    assert result.iloc[0]['value'] == 1
    assert result.iloc[1]['id'] == 'x'
    assert result.iloc[1]['value'] == 2


def test_deduplicate_exact_events_removes_duplicates():
    df = pd.DataFrame({
        'id': ['x', 'x'],
        'value': [1, 1]
    })

    result,_ = deduplicate_exact_events(df)

    assert len(result) == 1
    assert result.iloc[0]['id'] == 'x'
    assert result.iloc[0]['value'] == 1


# ------------------------------------------------------------
# UNSPARSABLE TIMESTAMPS CONTRACT
# ------------------------------------------------------------

def test_remove_unparsable_timestamps_passed(valid_orders_df):
    
    initial_count = len(valid_orders_df)
    result = remove_unparsable_timestamps(valid_orders_df)

    assert len(result) == initial_count


def test_remove_unparsable_timestamps_drops_invalid_rows(invalid_order_df):
    
    initial_count = len(invalid_order_df)
    result, _ = remove_unparsable_timestamps(invalid_order_df)

    assert len(result) < initial_count


# ------------------------------------------------------------
# IMPOSSIBLE TIMESTAMPS ORDER
# ------------------------------------------------------------

def test_remove_impossible_timestamps(valid_orders_df):
    
    initial_count = len(valid_orders_df)
    result = remove_impossible_timestamps(valid_orders_df)

    assert len(result) == initial_count
    
    
def test_remove_impossible_timestamps_drops_invalid_rows(invalid_temporal_order_df):
    
    initial_count = len(invalid_temporal_order_df)
    result, _ = remove_impossible_timestamps(invalid_temporal_order_df)
    
    assert len(result) < initial_count


# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------

def test_apply_contract_event_fact_success(tmp_path):

    raw_dir = tmp_path / 'raw'
    raw_dir.mkdir()

    df = pd.DataFrame({
        'order_id': [1, 2, 3, 4],
        'order_purchase_timestamp': [
            '2026-01-01',
            '2026-01-01',  # duplicate
            'bad_timestamp', # unparsable
            '2026-01-03'
        ],
        'order_approved_at': [
            '2026-01-01',
            '2026-01-01',
            '2026-01-02',
            '2025-12-01'  # impossible (before purchase)
        ],
        'order_delivered_timestamp': [
            '2026-01-05',
            '2026-01-05',
            '2026-01-06',
            '2026-01-02'
        ],
        'order_estimated_delivery_date': [
            '2026-01-06',
            '2026-01-06',
            '2026-01-07',
            '2026-01-04'
        ],
    })

    df.to_csv(raw_dir / 'df_orders_2026_01.csv', index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_contract(run_context, 'df_orders')

    assert report['deduplicated_rows'] == 0
    assert report['removed_unparsable_timestamps'] == 1
    assert report['removed_impossible_timestamps'] == 1
    assert report['final_rows'] == 2

    output_file = run_context.contracted_path / 'df_orders_contracted.parquet'
    
    assert output_file.exists()


def test_apply_contract_unknown_table(tmp_path):

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    report = apply_contract(run_context, 'unknown_table')

    assert report['status'] == 'failed'
    assert 'Unknown table' in report['errors'][0]
    
    
def test_apply_contract_duplicate_pk(tmp_path):

    raw_dir = tmp_path / 'raw'
    raw_dir.mkdir()

    df = pd.DataFrame({
        'order_id': [1, 1],  # duplicate PK
        'order_purchase_timestamp': ['2026-01-01', '2026-01-02'],
        'order_approved_at': ['2026-01-01', '2026-01-02'],
        'order_delivered_timestamp': ['2026-01-03', '2026-01-04'],
        'order_estimated_delivery_date': ['2026-01-05', '2026-01-06'],
    })

    df.to_csv(raw_dir / 'df_orders_2026_01.csv', index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_contract(run_context, 'df_orders')

    assert report['status'] == 'failed'
    assert 'Primary key violation' in report['errors'][0]
    
    
def test_apply_contract_missing_required_timestamp(tmp_path):

    raw_dir = tmp_path / 'raw'
    raw_dir.mkdir()

    df = pd.DataFrame({
        'order_id': [1],
        # Missing required timestamp columns
    })

    df.to_csv(raw_dir / 'df_orders_2026_01.csv', index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_contract(run_context, 'df_orders')

    assert report['status'] == 'failed'
    assert 'Missing required timestamp' in report['errors'][0]
    


# =============================================================================
# UNIT TESTS END
# =============================================================================