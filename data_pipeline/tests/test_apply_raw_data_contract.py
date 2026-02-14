# =============================================================================
# UNIT TESTS FOR apply_raw_data_contract.py
# =============================================================================

import pandas as pd
import pytest

from data_pipeline.stages import apply_raw_data_contract as module_contract
from data_pipeline.stages.apply_raw_data_contract import(
    validate_primary_key,
    validate_required_event_timestamps,
    deduplicate_exact_events,
    remove_unparsable_timestamps,
    remove_impossible_timestamps,
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

    result = deduplicate_exact_events(df)

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

    result = deduplicate_exact_events(df)

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
    result = remove_unparsable_timestamps(invalid_order_df)

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
    result = remove_impossible_timestamps(invalid_temporal_order_df)
    
    assert len(result) < initial_count


# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------

def test_apply_contract_halts_on_primary_key_failure(monkeypatch):
    
    # Dummy config
    monkeypatch.setattr(
        module_contract,
        'TABLE_CONFIG',
        {'df_orders': {'role': 'event_fact', 'primary_key': ['id']}}
    )

    # Force to fail in PK validation
    monkeypatch.setattr(
        module_contract,
        'load_logical_table',
        lambda *args, **kwargs: pd.DataFrame({'x': [1, 2]})
    )

    # Dummy exporter so it doesn't write
    monkeypatch.setattr(
        module_contract,
        'export_file',
        lambda *args, **kwargs: True
    )

    # Validation detected == sys.exit(1)
    with pytest.raises(SystemExit) as exc:
        module_contract.apply_contract('df_orders', 'train')
    
    # Halts enforcement
    assert exc.value.code == 1


def test_apply_contract_calls_export(monkeypatch, valid_orders_df):

    monkeypatch.setattr(
        module_contract,
        'TABLE_CONFIG',
        {'df_orders': {'role': 'event_fact', 'primary_key': ['order_id']}}
    )

    monkeypatch.setattr(
        module_contract,
        'load_logical_table',
        lambda *args, **kwargs: valid_orders_df
    )

    called = {'export': False}

    # Dummy exporter, return True for success
    def fake_export(*args, **kwargs):
        called['export'] = True
        return True

    monkeypatch.setattr(module_contract, 'export_file', fake_export)
    module_contract.apply_contract('df_orders', 'train')

    # Contract application completed
    assert called['export'] is True

# ------------------------------------------------------------
# MAIN EXECUTION (USER INPUT)
# ------------------------------------------------------------

def test_main_executes_apply_contract(monkeypatch):

    called = {'run': False}

    # Dummy contract application, return True if executed
    def fake_apply(table_name, partition):
        called['run'] = True

    monkeypatch.setattr(module_contract, 'apply_contract', fake_apply)

    monkeypatch.setattr(
        'sys.argv',
        ['script', '--table', 'df_orders', '--partition', 'train']
    )

    module_contract.main()

    # Script executed
    assert called['run'] is True


# =============================================================================
# UNIT TESTS END
# =============================================================================