# =============================================================================
# UNIT TESTS FOR assembly_executor.py
# =============================================================================

import polars as pl
import pytest
from pathlib import Path
from data_pipeline.shared.run_context import RunContext
from data_pipeline.assembly.assembly_logic import log_info, log_error, init_report
from data_pipeline.assembly.assembly_executor import (
    merge_data,
    derive_fields,
    freeze_schema,
    assemble_events,
)
from data_pipeline.shared.modeling_configs import ASSEMBLE_DTYPES
from data_pipeline.assembly.assembly_logic import dimension_references


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_orders_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_id_int": [1, 2],
            "customer_id": ["cos1", "cos2"],
            "customer_id_int": [101, 102],
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
    ).with_columns(
        [
            pl.col("order_purchase_timestamp").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S"
            ),
            pl.col("order_approved_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("order_delivered_timestamp").str.strptime(
                pl.Datetime, "%Y-%m-%d %H:%M:%S"
            ),
            pl.col("order_estimated_delivery_date")
            .str.strptime(pl.Date, "%Y-%m-%d")
            .cast(pl.Datetime),
            pl.col("order_id_int").cast(pl.UInt32),
            pl.col("customer_id_int").cast(pl.UInt32),
        ]
    )


@pytest.fixture
def valid_order_items_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_id_int": [1, 2],
            "product_id": ["prod1", "prod2"],
            "product_id_int": [201, 202],
            "seller_id": ["seller1", "seller2"],
            "seller_id_int": [301, 302],
            "price": [12.3, 45.6],
            "shipping_charges": [1.23, 4.56],
        }
    ).with_columns(
        [
            pl.col("order_id_int").cast(pl.UInt32),
            pl.col("product_id_int").cast(pl.UInt32),
            pl.col("seller_id_int").cast(pl.UInt32),
        ]
    )


@pytest.fixture
def valid_payments_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_id_int": [1, 2],
            "payment_sequential": [1, 2],
            "payment_type": ["credit", "cash"],
            "payment_installments": [4, 5],
            "payment_value": [100.1, 50.2],
        }
    ).with_columns([pl.col("order_id_int").cast(pl.UInt32)])


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
def valid_derived_df():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_id_int": [1, 2],
            "seller_id": ["seller1", "seller2"],
            "seller_id_int": [301, 302],
            "customer_id": ["cos1", "cos2"],
            "customer_id_int": [101, 102],
            "order_revenue": [100.1, 50.2],
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
            "order_year_week": ["2023-W01", "2023-W02"],
            "run_id": "20230101T120000",
        }
    )
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
            pl.col("order_date").str.strptime(pl.Date, "%Y-%m-%d").cast(pl.Datetime),
            pl.col("order_id_int").cast(pl.UInt32),
            pl.col("customer_id_int").cast(pl.UInt32),
            pl.col("product_id_int").cast(pl.UInt32),
            pl.col("seller_id_int").cast(pl.UInt32),
        ]
    )
    return df


# =============================================================================
# REPORTING & LOGS
# =============================================================================


def test_init_report_structure():
    report = init_report()
    assert report["status"] == ""
    assert "errors" in report
    assert "info" in report
    assert "loaded_data" in report


def test_log_error_appends_only_to_errors(empty_report):
    log_error("test error", empty_report)
    assert empty_report["errors"] == ["test error"]


def test_log_info_appends_only_to_info(empty_report):
    log_info("test info", empty_report)
    assert empty_report["info"] == ["test info"]


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
    if isinstance(result, pl.LazyFrame):
        result = result.collect()

    assert result.height == 2
    assert result.select(pl.col("order_id_int").is_duplicated().any()).item() == False
    assert "order_revenue" in result.columns


def test_merge_data_aggregates_duplicates(
    valid_orders_df,
    valid_order_items_df,
):

    duplicated_items_df = pl.concat(
        [valid_order_items_df, valid_order_items_df.slice(0, 1)]
    )

    assert (
        duplicated_items_df["order_id_int"][0] == duplicated_items_df["order_id_int"][2]
    )

    result = merge_data(
        {
            "df_orders": valid_orders_df,
            "df_order_items": duplicated_items_df,
            "df_payments": pl.DataFrame(
                {
                    "order_id": ["o1", "o2"],
                    "order_id_int": [1, 2],
                    "payment_value": [10.0, 20.0],
                }
            ).with_columns([pl.col("order_id_int").cast(pl.UInt32)]),
        }
    )
    if isinstance(result, pl.LazyFrame):
        result = result.collect()

    assert result.height == 2
    assert result.select(pl.col("order_id_int").is_duplicated().any()).item() == False


# =============================================================================
# DERIVING FIELDS
# =============================================================================


def test_derived_fields_correctness(valid_derived_df):
    result = derive_fields(valid_derived_df)

    if isinstance(result, pl.LazyFrame):
        result = result.collect()

    assert result["lead_time_days"].to_list() == [3, 5]
    assert result["approval_lag_days"].to_list() == [1, 1]
    assert result["delivery_delay_days"].to_list() == [1, 1]
    assert "order_year_week" in result.columns


# =============================================================================
# FREEZING SCHEMA
# =============================================================================


def test_freeze_schema_enforces_strict_schema_success(valid_derived_df):
    result = freeze_schema(valid_derived_df)

    if isinstance(result, pl.LazyFrame):
        result = result.collect()

    for col, expected_dtype in ASSEMBLE_DTYPES.items():
        assert result[col].dtype == expected_dtype


def test_freeze_schema_fails_on_missing_column(valid_derived_df):
    missing_required_column = valid_derived_df.drop("seller_id_int")
    with pytest.raises(RuntimeError, match="missing required columns"):
        result = freeze_schema(missing_required_column)
        if isinstance(result, pl.LazyFrame):
            result.collect()


# =============================================================================
# ASSEMBLING DATA
# =============================================================================


def test_assemble_data_success(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
    valid_customers_df,
    valid_products_df,
):
    run_id = "20230101T120000"
    run_context = RunContext.create(
        base=tmp_path, run_id=run_id, storage=tmp_path / "storage"
    )
    run_context.initialize_directories()
    storage_contracted_path = Path(run_context.storage_contracted_path)
    storage_contracted_path.mkdir(parents=True, exist_ok=True)

    valid_orders_df.write_parquet(storage_contracted_path / "df_orders.parquet")
    valid_order_items_df.write_parquet(
        storage_contracted_path / "df_order_items.parquet"
    )
    valid_payments_df.write_parquet(storage_contracted_path / "df_payments.parquet")
    valid_customers_df.write_parquet(storage_contracted_path / "df_customers.parquet")
    valid_products_df.write_parquet(storage_contracted_path / "df_products.parquet")

    report = assemble_events(run_context)

    assert report["status"] == "success"

    assert (run_context.assembled_path / "assembled_events_2023_01_01.parquet").exists()
    assert (run_context.assembled_path / "df_customers_2023_01_01.parquet").exists()
    assert (run_context.assembled_path / "df_products_2023_01_01.parquet").exists()


def test_assemble_data_fails_on_missing_column(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
):
    run_id = "20230101T120000"
    run_context = RunContext.create(
        base=tmp_path, run_id=run_id, storage=tmp_path / "storage"
    )
    run_context.initialize_directories()
    storage_contracted_path = Path(run_context.storage_contracted_path)
    storage_contracted_path.mkdir(parents=True, exist_ok=True)

    invalid_order_items_df = valid_order_items_df.drop("seller_id_int")

    valid_orders_df.write_parquet(storage_contracted_path / "df_orders.parquet")
    invalid_order_items_df.write_parquet(
        storage_contracted_path / "df_order_items.parquet"
    )
    valid_payments_df.write_parquet(storage_contracted_path / "df_payments.parquet")

    report = assemble_events(run_context)

    assert report["status"] == "failed"
    assert report["assembled_events"]["freeze_schema"] == False
    assert any(
        "missing required columns" in error
        or 'unable to find column "seller_id_int"' in error
        for error in report["errors"]
    )


# =============================================================================
# DIMENSION EXTRACTION
# =============================================================================


def test_dimension_references_uniqueness():
    df = pl.DataFrame({"id": ["1", "1", "2"], "val": ["a", "a", "b"]})
    df_dtypes = {"id": pl.String, "val": pl.String}

    result = dimension_references(df.lazy(), ["id"], ["id", "val"], df_dtypes)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    assert result.height == 2

    df_conflict = pl.DataFrame({"id": ["1", "1"], "val": ["a", "b"]})

    result = dimension_references(df_conflict.lazy(), ["id"], ["id", "val"], df_dtypes)
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    assert result.height == 1
