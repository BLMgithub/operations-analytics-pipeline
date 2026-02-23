# =============================================================================
# UNIT TESTS FOR validate_raw_data.py
# =============================================================================

import pandas as pd
import pytest

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.assemble_validated_events import (
    init_report,
    log_info,
    log_error,
    merge_data,
    derive_fields,
    freeze_schema,
    assemble_events,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_orders_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["cos1", "cos2"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": [
                "2023-01-02 09:00:00",
                "2023-01-10 14:00:00",
            ],
            "order_approved_at": [
                "2023-01-03 09:00:00",
                "2023-01-11 14:00:00",
            ],
            "order_delivered_timestamp": [
                "2023-01-06 09:00:00",
                "2023-01-16 14:00:00",
            ],
            "order_estimated_delivery_date": [
                "2023-01-05",
                "2023-01-15",
            ],
        }
    )


@pytest.fixture
def valid_order_items_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "product_id": ["prod1", "prod2"],
            "seller_id": ["seller1", "seller2"],
            "price": [12.3, 45.6],
            "shipping_charges": [1.23, 4.56],
        }
    )


@pytest.fixture
def valid_payments_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "payment_sequential": [1, 2],
            "payment_type": ["credit", "cash"],
            "payment_installments": [4, 5],
            "payment_value": [100.1, 50.2],
        }
    )


@pytest.fixture
def valid_derived_df():
    return pd.DataFrame(
        {
            "order_id": pd.Series(
                ["o1", "o2"],
                dtype="string",
            ),
            "seller_id": pd.Series(["seller1", "seller2"], dtype="string"),
            "product_id": pd.Series(["prod1", "prod2"], dtype="string"),
            "order_status": pd.Series(["delivered", "cancelled"], dtype="string"),
            "order_purchase_timestamp": pd.Series(
                [
                    "2023-01-02 09:00:00",
                    "2023-01-10 14:00:00",
                ],
                dtype="datetime64[ns]",
            ),
            "order_approved_at": pd.Series(
                [
                    "2023-01-03 09:00:00",
                    "2023-01-11 14:00:00",
                ],
                dtype="datetime64[ns]",
            ),
            "order_delivered_timestamp": pd.Series(
                [
                    "2023-01-06 09:00:00",
                    "2023-01-16 14:00:00",
                ],
                dtype="datetime64[ns]",
            ),
            "order_estimated_delivery_date": [
                "2023-01-05",
                "2023-01-15",
            ],
            "lead_time_days": pd.Series([3, 5], dtype="int64"),
            "approval_lag_days": pd.Series([1, 1], dtype="int64"),
            "delivery_delay_days": pd.Series([1, 1], dtype="int64"),
            "order_date": pd.Series(pd.to_datetime(["2023-01-02", "2023-01-10"]).date),
            "order_year": pd.Series([2023, 2023], dtype="int64"),
            "order_year_week": pd.Series(["2023-W01", "2023-W02"], dtype="string"),
            "run_id": "dummy_run_id",
        }
    )


# =============================================================================
# REPORTING & LOGS
# =============================================================================


def test_init_report_structure():
    report = init_report()

    assert set(report.keys()) == {"status", "errors", "info"}
    assert all(isinstance(v, list | str) for v in report.values())


def test_log_error_appends_only_to_errors(empty_report):
    log_error("errors", empty_report)

    assert empty_report["errors"] == ["errors"]


def test_log_info_appends_only_to_info(empty_report):
    log_info("info", empty_report)

    assert empty_report["info"] == ["info"]


# =============================================================================
# MERGING DATA
# =============================================================================


def test_merge_data_preserve_grain(
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
):

    result = merge_data(
        {
            "df_orders": valid_orders_df,
            "df_order_items": valid_order_items_df,
            "df_payments": valid_payments_df,
        }
    )

    assert len(result) == 2
    assert result.duplicated().any() == False


def test_merge_detects_cardinality_violation(
    valid_orders_df,
    valid_order_items_df,
):

    duplicated_payments_df = pd.DataFrame(
        {
            "order_id": ["o1", "o1"],
            "payment_sequential": [1, 1],
            "payment_type": ["credit", "credit"],
            "payment_installments": [4, 4],
            "payment_value": [100.1, 100.1],
        }
    )

    with pytest.raises(RuntimeError):
        merge_data(
            {
                "df_orders": valid_orders_df,
                "df_order_items": valid_order_items_df,
                "df_payments": duplicated_payments_df,
            }
        )


# =============================================================================
# DERIVING FIELDS
# =============================================================================


def test_derived_fields_correctness(valid_derived_df):

    source_cols = [
        "order_id",
        "seller_id",
        "product_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_timestamp",
        "order_estimated_delivery_date",
    ]

    expected_cols = [
        "lead_time_days",
        "approval_lag_days",
        "delivery_delay_days",
        "order_date",
        "order_year",
        "order_year_week",
        "run_id",
    ]

    source_df = valid_derived_df[source_cols].copy()
    expected_df = valid_derived_df[expected_cols].copy()

    result = derive_fields(source_df, "dummy_run_id")

    pd.testing.assert_frame_equal(
        result[expected_cols].reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False,
    )


# =============================================================================
# FREEZING SCHEMA
# =============================================================================


def test_freeze_schema_enforces_strict_schema_success(valid_derived_df):

    expected_dtypes = {
        "order_id": "string",
        "seller_id": "string",
        "product_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "datetime64[ns]",
        "order_approved_at": "datetime64[ns]",
        "order_delivered_timestamp": "datetime64[ns]",
        "lead_time_days": "int64",
        "approval_lag_days": "int64",
        "delivery_delay_days": "int64",
        "order_date": "object",
        "order_year": "int64",
    }

    result = freeze_schema(valid_derived_df)

    for col, correct_dtype in expected_dtypes.items():
        assert str(result[col].dtype) == correct_dtype


def test_freeze_schema_fails_on_missing_column(valid_derived_df):

    missing_required_column = valid_derived_df.drop(columns="seller_id")

    with pytest.raises(RuntimeError):
        freeze_schema(missing_required_column)


# =============================================================================
# ASSEMBLING DATA
# =============================================================================


def test_assemble_data_success(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    valid_orders_df.to_parquet(
        run_context.contracted_path / "df_orders_contracted.parquet",
        index=False,
    )
    valid_order_items_df.to_parquet(
        run_context.contracted_path / "df_order_items_contracted.parquet",
        index=False,
    )
    valid_payments_df.to_parquet(
        run_context.contracted_path / "df_payments_contracted.parquet",
        index=False,
    )

    report = assemble_events(run_context)

    assert report["status"] == "success"

    output_file = run_context.assembled_path / "assembled_events.parquet"

    assert output_file.exists()


def test_assemble_data_fails_on_missing_column(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    invalid_order_items_df = valid_order_items_df.drop(columns="seller_id")

    valid_orders_df.to_parquet(
        run_context.contracted_path / "df_orders_contracted.parquet",
        index=False,
    )

    invalid_order_items_df.to_parquet(
        run_context.contracted_path / "df_order_items_contracted.parquet",
        index=False,
    )

    valid_payments_df.to_parquet(
        run_context.contracted_path / "df_payments_contracted.parquet", index=False
    )

    report = assemble_events(run_context)
    output_file = run_context.assembled_path / "assembled_events.parquet"

    assert report["status"] == "failed"
    assert output_file.exists() == False
    assert any(
        "missing required columns: ['seller_id']" in error for error in report["errors"]
    )


def test_assemble_data_fails_on_cardinality(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
):

    duplicated_payments_df = pd.DataFrame(
        {
            "order_id": ["o1", "o1"],
            "payment_sequential": [1, 1],
            "payment_type": ["credit", "credit"],
            "payment_installments": [4, 4],
            "payment_value": [100.1, 100.1],
        }
    )

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    valid_orders_df.to_parquet(
        run_context.contracted_path / "df_orders_contracted.parquet",
        index=False,
    )

    valid_order_items_df.to_parquet(
        run_context.contracted_path / "df_order_items_contracted.parquet",
        index=False,
    )

    duplicated_payments_df.to_parquet(
        run_context.contracted_path / "df_payments_contracted.parquet", index=False
    )

    report = assemble_events(run_context)
    output_file = run_context.assembled_path / "assembled_events.parquet"

    assert report["status"] == "failed"
    assert output_file.exists() == False
    assert any(
        "Cardinality violation detected: expected 1 row per order" in error
        for error in report["errors"]
    )


# =============================================================================
# UNIT TESTS END
# =============================================================================
