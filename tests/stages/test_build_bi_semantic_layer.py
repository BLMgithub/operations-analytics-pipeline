# =============================================================================
# UNIT TESTS FOR build_bi_sematic_layer.py
# =============================================================================

import pandas as pd
import pytest

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.build_bi_semantic_layer import (
    init_report,
    log_error,
    log_info,
    seller_weekly_semantic,
    freeze_seller_semantic,
    build_semantic_layer,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_assembled_df():
    return pd.DataFrame(
        {
            "order_id": pd.Series(
                ["o1", "o2"],
                dtype="string",
            ),
            "seller_id": pd.Series(["seller1", "seller2"], dtype="string"),
            "order_revenue": pd.Series([12.34, 56.78], dtype="float64"),
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
            "order_date": pd.Series(
                ["2023-01-02", "2023-01-10"], dtype="datetime64[ns]"
            ),
            "order_year": pd.Series([2023, 2023], dtype="int64"),
            "order_year_week": pd.Series(["2023-W01", "2023-W01"], dtype="string"),
            "run_id": ["dummy_run_id", "dummy_run_id"],
        }
    )


@pytest.fixture
def valid_seller_fact():
    return pd.DataFrame(
        {
            "seller_id": pd.Series(["seller1", "seller2"], dtype="string"),
            "order_year_week": pd.Series(["2023-W01", "2023-W02"], dtype="string"),
            "week_start_date": pd.Series(
                ["2023-01-02 09:00:00", "2023-01-04 15:00:00"], dtype="datetime64[ns]"
            ),
            "run_id": pd.Series(["run_1", "run_1"], dtype="string"),
            "weekly_order_count": pd.Series([12, 34], dtype="int64"),
            "weekly_delivered_orders": pd.Series([5, 6], dtype="int64"),
            "weekly_cancelled_orders": pd.Series([7, 8], dtype="int64"),
            "weekly_revenue": pd.Series([12.3, 45.6], dtype="float64"),
            "weekly_avg_lead_time": pd.Series([5.34, 6.45], dtype="float64"),
            "weekly_total_lead_time": pd.Series([5, 6], dtype="int64"),
            "weekly_avg_delivery_delay": pd.Series([54.50, 67.89], dtype="float64"),
            "weekly_total_delivery_delay": pd.Series([10, 11], dtype="int64"),
            "weekly_avg_approval_lag": pd.Series([12.3, 14.5], dtype="float64"),
        }
    )


@pytest.fixture
def valid_seller_dim():
    return pd.DataFrame(
        {
            "seller_id": pd.Series(["seller1", "seller2"], dtype="string"),
            "first_order_date": pd.Series(
                ["2023-03-02 09:00:00", "2023-05-04 11:00:00"], dtype="datetime64[ns]"
            ),
            "first_order_year_week": pd.Series(
                ["2023-W03", "2023-W05"], dtype="string"
            ),
            "run_id": pd.Series(["run_1", "run_1"], dtype="string"),
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
    log_error("error", empty_report)

    assert empty_report["errors"] == ["error"]


def test_log_info_appends_only_to_info(empty_report):
    log_info("info", empty_report)

    assert empty_report["info"] == ["info"]


# =============================================================================
# SELLER WEEKLY SEMANTIC MODELING AND SCHEMA ENFORCEMENT
# =============================================================================


def test_seller_semantic_model_grain_preserved_success(valid_assembled_df):

    seller_fact, seller_dim = seller_weekly_semantic(valid_assembled_df)
    expected = (
        valid_assembled_df[["seller_id", "order_year_week"]].drop_duplicates().shape[0]
    )

    # Fact preserved grain
    assert len(seller_fact) == expected

    # Dimension preserved grain
    assert len(seller_dim) == valid_assembled_df["seller_id"].drop_duplicates().shape[0]


def test_seller_semantic_fails_on_multiple_run_ids(valid_assembled_df):

    broken_df = valid_assembled_df.copy()
    broken_df.loc[1, "run_id"] = "another_run"

    with pytest.raises(RuntimeError):
        seller_weekly_semantic(broken_df)


def test_freeze_seller_semantic_fact_success(valid_seller_fact):

    result_fact = freeze_seller_semantic(valid_seller_fact, "fact")

    expected_dtypes = {
        "seller_id": "string",
        "order_year_week": "string",
        "week_start_date": "datetime64[ns]",
        "run_id": "string",
        "weekly_order_count": "int64",
        "weekly_delivered_orders": "int64",
        "weekly_cancelled_orders": "int64",
        "weekly_revenue": "float64",
        "weekly_avg_lead_time": "float64",
        "weekly_total_lead_time": "int64",
        "weekly_avg_delivery_delay": "float64",
        "weekly_total_delivery_delay": "int64",
        "weekly_avg_approval_lag": "float64",
    }

    assert list(result_fact.columns) == [
        "seller_id",
        "order_year_week",
        "week_start_date",
        "run_id",
        "weekly_order_count",
        "weekly_delivered_orders",
        "weekly_cancelled_orders",
        "weekly_revenue",
        "weekly_avg_lead_time",
        "weekly_total_lead_time",
        "weekly_avg_delivery_delay",
        "weekly_total_delivery_delay",
        "weekly_avg_approval_lag",
    ]

    for col, correct_dtypes in expected_dtypes.items():
        assert str(result_fact[col].dtype) == correct_dtypes

    assert result_fact.equals(
        result_fact.sort_values(["seller_id", "order_year_week"]).reset_index(drop=True)
    )

    assert len(result_fact) == len(valid_seller_fact)


def test_freeze_seller_semantic_dimension_success(valid_seller_dim):

    result_dim = freeze_seller_semantic(valid_seller_dim, "dim")

    assert list(result_dim.columns) == [
        "seller_id",
        "first_order_date",
        "first_order_year_week",
        "run_id",
    ]

    assert result_dim["seller_id"].dtype == "string"
    assert result_dim["first_order_date"].dtype == "datetime64[ns]"
    assert result_dim["first_order_year_week"].dtype == "string"
    assert result_dim["run_id"].dtype == "string"

    assert len(result_dim) == len(valid_seller_dim)


def test_freeze_seller_semantic_fact_fails_on_missing_column(valid_seller_fact):

    broken_seller_fact = valid_seller_fact.copy()
    broken_seller_fact.drop(columns="weekly_order_count", inplace=True)

    with pytest.raises(RuntimeError):
        freeze_seller_semantic(broken_seller_fact, "fact")


def test_freeze_seller_semantic_dimension_fails_on_missing_column(valid_seller_dim):

    broken_seller_dim = valid_seller_dim.copy()
    broken_seller_dim.drop(columns="first_order_year_week", inplace=True)

    with pytest.raises(RuntimeError):
        freeze_seller_semantic(broken_seller_dim, "dim")


def test_freeze_seller_semantic_fact_fails_on_duplictes(valid_seller_fact):

    broken_seller_fact = valid_seller_fact.copy()
    broken_seller_fact.loc[1, "seller_id"] = "seller1"
    broken_seller_fact.loc[1, "order_year_week"] = "2023-W01"

    with pytest.raises(RuntimeError):
        freeze_seller_semantic(broken_seller_fact, "fact")


def test_freeze_seller_semantic_fact_dimension_on_duplictes(valid_seller_dim):

    broken_seller_dim = valid_seller_dim.copy()
    broken_seller_dim.loc[1, "seller_id"] = "seller1"

    with pytest.raises(RuntimeError):
        freeze_seller_semantic(broken_seller_dim, "dim")


def test_freeze_seller_semantic_fails_on_table_type(valid_seller_fact):

    with pytest.raises(ValueError):
        freeze_seller_semantic(valid_seller_fact, "invalid_param")  # type: ignore


# =============================================================================
# BUILD BI SEMANTIC
# =============================================================================


def test_build_semantic_layer_success(tmp_path, valid_assembled_df):

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    valid_assembled_df.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01.parquet"
    )

    report = build_semantic_layer(run_context)

    output_path_seller = (
        run_context.semantic_path / "seller_week_performance_fact_dumm_y_.parquet"
    )

    output_path_dim = run_context.semantic_path / "dim_seller_dumm_y_.parquet"

    assert report["status"] == "success"
    assert output_path_seller.exists()
    assert output_path_dim.exists()


def test_build_semantic_layer_fails_on_multiple_ids(tmp_path, valid_assembled_df):

    broken_assembled = valid_assembled_df.copy()
    broken_assembled.loc[1, "run_id"] = "another_run"

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    broken_assembled.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    assert "Multiple run_ids detected" in report["errors"]


def test_build_semantic_layer_fails_on_missing_columns(tmp_path, valid_assembled_df):

    broken_assembled = valid_assembled_df.copy()
    broken_assembled.drop(columns="approval_lag_days", inplace=True)

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    broken_assembled.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    assert any("approval_lag_days" in error for error in report["errors"])


# =============================================================================
# UNIT TESTS END
# =============================================================================
