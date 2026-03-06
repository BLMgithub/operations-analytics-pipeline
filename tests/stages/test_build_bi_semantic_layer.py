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
    SEMANTIC_MODULES,
    build_seller_semantic,
    build_semantic_layer,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_customers_df():
    return pd.DataFrame(
        {
            "customer_id": pd.Series(["customer1", "customer2"], dtype="string"),
            "customer_zip_code_prefix": pd.Series(["zip1", "zip2"], dtype="string"),
            "customer_city": pd.Series(["city1", "city2"], dtype="string"),
            "customer_state": pd.Series(["state1", "state2"], dtype="string"),
        }
    )


@pytest.fixture
def valid_products_df():
    return pd.DataFrame(
        {
            "product_id": pd.Series(["prod1", "prod2"], dtype="string"),
            "product_category_name": pd.Series(["categ1", "categ2"], dtype="string"),
            "product_weight_g": pd.Series([491, 500], dtype="float64"),
            "product_length_cm": pd.Series([19.0, 20.0], dtype="float64"),
            "product_height_cm": pd.Series([12.0, 13.0], dtype="float64"),
            "product_width_cm": pd.Series([16.0, 15.0], dtype="float64"),
        }
    )


@pytest.fixture
def valid_assembled_df():
    return pd.DataFrame(
        {
            "order_id": pd.Series(
                ["o1", "o2"],
                dtype="string",
            ),
            "seller_id": pd.Series(["seller1", "seller2"], dtype="string"),
            "customer_id": pd.Series(["customer1", "customer2"], dtype="string"),
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


def test_seller_semantic_model_grain_preserved_success(tmp_path, valid_assembled_df):

    run_context = RunContext.create(base_path=tmp_path)

    seller_semantic = build_seller_semantic(valid_assembled_df, run_context)
    expected = (
        valid_assembled_df[["seller_id", "order_year_week"]].drop_duplicates().shape[0]
    )

    # Fact preserved grain
    assert len(seller_semantic["seller_weekly_fact"]) == expected

    # Dimension preserved grain
    assert (
        len(seller_semantic["seller_dim"])
        == valid_assembled_df["seller_id"].drop_duplicates().shape[0]
    )


def test_seller_semantic_fails_on_multiple_run_ids(tmp_path, valid_assembled_df):

    run_context = RunContext.create(base_path=tmp_path)

    broken_df = valid_assembled_df.copy()
    broken_df.loc[1, "run_id"] = "another_run"

    with pytest.raises(RuntimeError):
        build_seller_semantic(broken_df, run_context)


# =============================================================================
# BUILD BI SEMANTIC
# =============================================================================


def test_build_semantic_layer_success(
    tmp_path,
    valid_assembled_df,
    valid_customers_df,
    valid_products_df,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    valid_assembled_df.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01.parquet"
    )

    valid_customers_df.to_parquet(
        run_context.contracted_path / "df_customers_contracted.parquet"
    )

    valid_products_df.to_parquet(
        run_context.contracted_path / "df_products_contracted.parquet"
    )

    report = build_semantic_layer(run_context)

    for module_name, module in SEMANTIC_MODULES.items():
        for table_name in module["tables"]:

            outputs_path = (
                run_context.semantic_path
                / module_name
                / f"{table_name}_dumm_y_.parquet"
            )

            assert report["status"] == "success"
            assert outputs_path.exists()


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
    assert report["failed_module"] == "seller_semantic"

    module_error = report["modules"]["seller_semantic"]["errors"]

    assert any("Multiple run_ids detected" in error for error in module_error)


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
    assert report["failed_module"] == "seller_semantic"

    module_error = report["modules"]["seller_semantic"]["errors"]

    assert any("approval_lag_days" in error for error in module_error)


def test_build_semantic_layer_fails_on_missing_or_empty_df(tmp_path):

    empty_df = pd.DataFrame()

    run_context = RunContext.create(base_path=tmp_path, run_id="dummy_run_id")
    run_context.initialize_directories()

    empty_df.to_parquet(run_context.assembled_path / "assembled_events_2023_01.parquet")

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    assert report["failed_step"] == "load_tables"

    load_error = report["steps"]["load_tables"]["errors"]

    assert any("missing or empty" in error for error in load_error)


# =============================================================================
# UNIT TESTS END
# =============================================================================
