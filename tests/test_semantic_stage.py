# =============================================================================
# UNIT TESTS FOR semantic_executor.py
# =============================================================================

import pandas as pd
import pytest
from data_pipeline.shared.run_context import RunContext
from data_pipeline.semantic.semantic_executor import (
    init_report,
    log_error,
    log_info,
    SEMANTIC_MODULES,
    build_semantic_layer,
)
from data_pipeline.semantic.semantic_logic import (
    build_seller_semantic,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_customers_df():
    return pd.DataFrame(
        {
            "customer_id": ["cos1", "cos2"],
            "customer_state": ["SP", "RJ"],
        }
    )


@pytest.fixture
def valid_products_df():
    return pd.DataFrame(
        {
            "product_id": ["prod1", "prod2"],
            "product_category_name": ["tech", "home"],
            "product_weight_g": [100.0, 500.0],
        }
    )


@pytest.fixture
def valid_assembled_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "seller_id": ["seller1", "seller2"],
            "customer_id": ["cos1", "cos2"],
            "order_revenue": [12.34, 56.78],
            "product_id": ["prod1", "prod2"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": pd.to_datetime(
                [
                    "2023-01-02 09:00:00",
                    "2023-01-10 14:00:00",
                ]
            ),
            "order_approved_at": pd.to_datetime(
                [
                    "2023-01-03 09:00:00",
                    "2023-01-11 14:00:00",
                ]
            ),
            "order_delivered_timestamp": pd.to_datetime(
                [
                    "2023-01-06 09:00:00",
                    "2023-01-16 14:00:00",
                ]
            ),
            "order_estimated_delivery_date": pd.to_datetime(
                [
                    "2023-01-05",
                    "2023-01-15",
                ]
            ),
            "lead_time_days": [3, 5],
            "approval_lag_days": [1, 1],
            "delivery_delay_days": [1, 1],
            "order_date": pd.to_datetime(["2023-01-02", "2023-01-10"]),
            "order_year": [2023, 2023],
            "order_year_week": ["2023-W01", "2023-W01"],
            "run_id": ["20230101T120000", "20230101T120000"],
        }
    ).astype(
        {
            "order_status": "category",
            "lead_time_days": "int16",
            "approval_lag_days": "int16",
            "delivery_delay_days": "int16",
            "order_year": "int16",
            "order_revenue": "float32",
        }
    )


# =============================================================================
# REPORTING & LOGS
# =============================================================================


def test_init_report_structure():
    report = init_report()
    assert set(report.keys()) == {"status", "errors", "info"}
    assert report["status"] == "success"


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
    run_context = RunContext.create(base=tmp_path, run_id="20230101T120000")
    seller_semantic = build_seller_semantic(valid_assembled_df, run_context)

    expected_fact_len = (
        valid_assembled_df[["seller_id", "order_year_week"]].drop_duplicates().shape[0]
    )
    assert len(seller_semantic["seller_weekly_fact"]) == expected_fact_len

    expected_dim_len = valid_assembled_df["seller_id"].nunique()
    assert len(seller_semantic["seller_dim"]) == expected_dim_len


def test_seller_semantic_fails_on_multiple_run_ids(tmp_path, valid_assembled_df):
    run_context = RunContext.create(base=tmp_path, run_id="20230101T120000")
    broken_df = valid_assembled_df.copy()
    broken_df.loc[1, "run_id"] = "another_run"

    with pytest.raises(RuntimeError, match="Multiple run_ids detected"):
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
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    # Setup Assembled layer
    valid_assembled_df.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )
    valid_customers_df.to_parquet(
        run_context.assembled_path / "df_customers_2023_01_01.parquet"
    )
    valid_products_df.to_parquet(
        run_context.assembled_path / "df_products_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "success"

    for module_name, module_config in SEMANTIC_MODULES.items():
        for table_name in module_config["tables"]:
            outputs_path = (
                run_context.semantic_path
                / module_name
                / f"{table_name}_2023_01_01.parquet"
            )
            assert outputs_path.exists()


def test_build_semantic_layer_fails_on_multiple_ids(tmp_path, valid_assembled_df):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    broken_assembled = valid_assembled_df.copy()
    broken_assembled.loc[1, "run_id"] = "another_run"

    broken_assembled.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    assert report["modules"]["seller_semantic"]["build_stage"]["status"] == "failed"
    assert any(
        "Multiple run_ids detected" in error
        for error in report["modules"]["seller_semantic"]["build_stage"]["errors"]
    )


def test_build_semantic_layer_fails_on_missing_columns(tmp_path, valid_assembled_df):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    broken_assembled = valid_assembled_df.copy()
    broken_assembled.drop(
        columns="order_revenue", inplace=True
    )  # Used in seller_weekly_fact

    broken_assembled.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    # Errors might happen during build_stage if it uses the column
    # or during validate_and_freeze if it's in the schema.
    # build_seller_semantic uses order_revenue for aggregation.
    assert report["modules"]["seller_semantic"]["build_stage"]["status"] == "failed"


def test_build_semantic_layer_fails_on_missing_or_empty_df(tmp_path):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    empty_df = pd.DataFrame()
    empty_df.to_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
    # load_single_delta itself succeeds even if DF is empty
    assert any(
        "missing or empty" in error
        for error in report["steps"]["load_tables"]["errors"]
    )
