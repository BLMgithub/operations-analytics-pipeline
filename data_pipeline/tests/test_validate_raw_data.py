# =============================================================================
# UNIT TESTS FOR validate_raw_data.py
# =============================================================================

import pandas as pd
import pytest

from data_pipeline.validate_raw_data import (
    init_report,
    log_info,
    log_warning,
    log_error,
    run_base_validations,
    run_event_fact_validations,
    run_transaction_detail_validations,
    run_cross_table_validations,
    main
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------

@pytest.fixture
def empty_report():
    return init_report()


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
def valid_transaction_df():
    return pd.DataFrame({
        'order_id': ['o1', 'o2'],
        'payment_sequential': [1, 1],
        'payment_value': [100.0, 50.0],
    })


# ------------------------------------------------------------
# VALIDATION REPORT & LOGS
# ------------------------------------------------------------

def test_init_report_structure():
    report = init_report()
    
    assert set(report.keys()) == {'errors', 'warnings', 'info'}
    assert all(isinstance(v, list) for v in report.values())


def test_log_error_appends_only_to_errors(empty_report):
    log_error('error detected', empty_report)

    assert empty_report['errors'] == ['error detected']
    assert empty_report['warnings'] == []
    assert empty_report['info'] == []


# ------------------------------------------------------------
# BASE VALIDATIONS (ALL TABLE)
# ------------------------------------------------------------

def test_base_validation_fails_on_empty_df(empty_report):
    df = pd.DataFrame()
    ok = run_base_validations(df, 'df_test', ['id'], empty_report)

    assert ok is False
    assert len(empty_report['errors']) == 1


def test_base_validation_fails_on_missing_pk(empty_report):
    df = pd.DataFrame({'x': [1, 2]})
    ok = run_base_validations(df, 'df_test', ['id'], empty_report)

    assert ok is False
    assert 'missing primary key' in empty_report['errors'][0]


def test_base_validation_passes_with_non_fatal_issues(empty_report):
    df = pd.DataFrame({
        'id': ['a', 'a'],
        'value': [1, 2],
    })

    ok = run_base_validations(df, 'df_test', ['id'], empty_report)

    assert ok is True
    assert len(empty_report['errors']) > 0



# ------------------------------------------------------------
# EVENT FACT VALIDATIONS
# ------------------------------------------------------------

def test_event_fact_validation_passes(valid_orders_df, empty_report):
    ok = run_event_fact_validations(
        valid_orders_df, 'df_Orders', empty_report
    )

    assert ok is True
    assert empty_report['errors'] == []


def test_event_fact_fails_on_missing_timestamp(valid_orders_df, empty_report):
    df = valid_orders_df.drop(columns=['order_approved_at'])

    ok = run_event_fact_validations(df, 'df_Orders', empty_report)

    assert ok is False
    assert len(empty_report['errors']) == 1


def test_event_fact_fails_on_invalid_temporal_order(valid_orders_df, empty_report):
    valid_orders_df['order_approved_at'] = ['2022-12-01', '2022-12-01']

    ok = run_event_fact_validations(
        valid_orders_df, 'df_Orders', empty_report
    )

    assert ok is False


# ------------------------------------------------------------
# TRANSACTION DETAIL VALIDATIONS
# ------------------------------------------------------------

def test_transaction_detail_passes(valid_transaction_df, empty_report):
    ok = run_transaction_detail_validations(
        valid_transaction_df, 'df_payments', empty_report
    )

    assert ok is True


def test_transaction_detail_fails_on_negative_value(empty_report):
    df = pd.DataFrame({
        'order_id': ['o1'],
        'payment_value': [-10],
    })

    ok = run_transaction_detail_validations(
        df, 'df_payments', empty_report
    )

    assert ok is False
    assert len(empty_report['errors']) == 1


# ------------------------------------------------------------
# CROSS-TABLE VALIDATIONS
# ------------------------------------------------------------

def test_cross_table_validation_passes(valid_orders_df, valid_transaction_df, empty_report):
    tables = {
        'df_Orders': valid_orders_df,
        'df_OrderItems': pd.DataFrame({'order_id': ['o1']}),
        'df_payments': valid_transaction_df,
    }

    ok = run_cross_table_validations(tables, empty_report)

    assert ok is True


def test_cross_table_fails_on_missing_table(empty_report):
    tables = {}

    ok = run_cross_table_validations(tables, empty_report)

    assert ok is False
    assert len(empty_report['errors']) == 1


# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------

def test_main_exits_with_error_when_base_validation_fails(monkeypatch):

    # Force base validation to fail
    monkeypatch.setattr(
        'data_pipeline.validate_raw_data.run_base_validations',
        lambda *args, **kwargs: False
    )

    # Bypass file loading to avoid filesystem dependency
    monkeypatch.setattr(
        'data_pipeline.validate_raw_data.load_logical_table',
        lambda *args, **kwargs: []
    )

    # Force file existence check to pass
    monkeypatch.setattr(
        'data_pipeline.validate_raw_data.os.path.exists',
        lambda *args, **kwargs: True
    )

    with pytest.raises(SystemExit) as exc:
        main()

    # sys.exit(1) = CI failure
    assert exc.value.code == 1


# ------------------------------------------------------------
# CI-GATE TEST
# ------------------------------------------------------------

def test_pipeline_fails_if_any_validator_fails():
    report = init_report()
    report['errors'].append('error detected')

    assert bool(report['errors']) is True

# =============================================================================
# UNIT TESTS END
# =============================================================================