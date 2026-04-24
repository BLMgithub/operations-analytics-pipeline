# =============================================================================
# UNIT TESTS FOR semantic_executor.py
# =============================================================================

import polars as pl
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
    return pl.DataFrame(
        {
            "customer_id": ["cos1", "cos2"],
            "customer_id_int": [101, 102],
            "customer_state": ["SP", "RJ"],
            "customer_city": ["Sao Paulo", "Rio"],
            "customer_segment": ["A", "B"],
            "account_creation_date": ["2022-01-01", "2022-01-01"],
        }
    ).with_columns(
        [
            pl.col("account_creation_date").str.strptime(pl.Datetime, "%Y-%m-%d"),
            pl.col("customer_id_int").cast(pl.UInt32),
        ]
    )


@pytest.fixture
def valid_products_df():
    return pl.DataFrame(
        {
            "product_id": ["prod1", "prod2"],
            "product_id_int": [201, 202],
            "product_category_name": ["tech", "home"],
            "product_weight_g": [100.0, 500.0],
            "product_length_cm": [10.0, 20.0],
            "product_height_cm": [5.0, 10.0],
            "product_width_cm": [5.0, 10.0],
            "product_fragility_index": ["Low", "High"],
            "supplier_tier": ["Gold", "Silver"],
        }
    ).with_columns([pl.col("product_id_int").cast(pl.UInt32)])


@pytest.fixture
def valid_assembled_df():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_id_int": [1, 2],
            "seller_id": ["seller1", "seller2"],
            "seller_id_int": [301, 302],
            "customer_id": ["cos1", "cos2"],
            "customer_id_int": [101, 102],
            "order_revenue": [12.34, 56.78],
            "product_id": ["prod1", "prod2"],
            "product_id_int": [201, 202],
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
            "lead_time_days": [3, 5],
            "approval_lag_days": [1, 1],
            "delivery_delay_days": [1, 1],
            "order_date": ["2023-01-02", "2023-01-10"],
            "order_year": [2023, 2023],
            "order_year_week": ["2023-W01", "2023-W01"],
            "run_id": ["20230101T120000", "20230101T120000"],
            # Extra columns required by join in semantic stage
            "customer_state": ["SP", "RJ"],
            "customer_city": ["Sao Paulo", "Rio"],
            "customer_segment": ["A", "B"],
            "account_creation_date": ["2022-01-01", "2022-01-01"],
        }
    )
    # Temporal casting for Polars
    df = df.with_columns(
        [
            pl.col("order_purchase_timestamp").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S"
            ),
            pl.col("order_approved_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("order_delivered_timestamp").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S"
            ),
            pl.col("order_estimated_delivery_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("order_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("account_creation_date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("order_status").cast(pl.Categorical),
            pl.col("lead_time_days").cast(pl.Int16),
            pl.col("approval_lag_days").cast(pl.Int16),
            pl.col("delivery_delay_days").cast(pl.Int16),
            pl.col("order_year").cast(pl.Int16),
            pl.col("order_revenue").cast(pl.Float32),
            pl.col("run_id").cast(pl.Categorical),
            pl.col("order_id_int").cast(pl.UInt32),
            pl.col("seller_id_int").cast(pl.UInt32),
            pl.col("customer_id_int").cast(pl.UInt32),
            pl.col("product_id_int").cast(pl.UInt32),
        ]
    )
    return df


# =============================================================================
# REPORTING & LOGS
# =============================================================================


def test_init_report_structure():
    report = init_report()
    assert set(report.keys()) == {"status", "errors", "info", "loaded_data"}
    assert report["status"] == ""


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
    seller_semantic = build_seller_semantic(valid_assembled_df.lazy(), run_context)

    expected_fact_len = (
        valid_assembled_df.select(["seller_id_int", "order_year_week"]).unique().height
    )

    fact_df = seller_semantic["seller_weekly_fact"]
    if isinstance(fact_df, pl.LazyFrame):
        fact_df = fact_df.collect()
    assert fact_df.height == expected_fact_len

    dim_df = seller_semantic["seller_dim"]
    if isinstance(dim_df, pl.LazyFrame):
        dim_df = dim_df.collect()
    expected_dim_len = valid_assembled_df["seller_id_int"].n_unique()
    assert dim_df.height == expected_dim_len


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

    valid_assembled_df.write_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )
    valid_customers_df.write_parquet(
        run_context.assembled_path / "df_customers_2023_01_01.parquet"
    )
    valid_products_df.write_parquet(
        run_context.assembled_path / "df_products_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == ""

    for module_name, module_config in SEMANTIC_MODULES.items():
        for table_name in module_config["tables"]:
            outputs_path = (
                run_context.semantic_path
                / module_name
                / f"{table_name}_2023_01_01.parquet"
            )
            assert outputs_path.exists()


def test_build_semantic_layer_fails_on_missing_columns(tmp_path, valid_assembled_df):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    broken_assembled = valid_assembled_df.drop("order_revenue")

    broken_assembled.write_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"


def test_build_semantic_layer_fails_on_missing_or_empty_df(tmp_path):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    empty_df = pl.DataFrame({"run_id": []}, schema={"run_id": pl.Categorical})

    empty_df.write_parquet(
        run_context.assembled_path / "assembled_events_2023_01_01.parquet"
    )

    report = build_semantic_layer(run_context)

    assert report["status"] == "failed"
