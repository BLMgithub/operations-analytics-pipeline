# =============================================================================
# UNIT TESTS FOR validation_logic.py and validation_executor.py
# =============================================================================

import polars as pl
import pytest
from data_pipeline.shared.run_context import RunContext
from data_pipeline.validation.validation_executor import apply_validation
from data_pipeline.validation.validation_logic import (
    init_report,
    log_info,
    log_warning,
    log_error,
    run_base_validations,
    run_event_fact_validations,
    run_transaction_detail_validations,
    run_cross_table_validations,
)

# =============================================================================
# FIXTURES (REFACETORED SCHEMAS)
# =============================================================================


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_orders_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c2"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": ["2026-03-25 10:00:00", "2026-03-25 11:00:00"],
            "order_approved_at": ["2026-03-25 10:05:00", "2026-03-25 11:05:00"],
            "order_delivered_timestamp": ["2026-03-27 10:00:00", "2026-03-27 11:00:00"],
            "order_estimated_delivery_date": ["2026-03-28", "2026-03-28"],
        }
    )


@pytest.fixture
def valid_order_items_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "product_id": ["p1", "p2"],
            "seller_id": ["s1", "s2"],
            "price": [100.0, 200.0],
        }
    )


@pytest.fixture
def valid_payments_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "payment_sequential": [1, 1],
            "payment_value": [100.0, 200.0],
        }
    )


@pytest.fixture
def valid_customers_df():
    return pl.DataFrame(
        {
            "customer_id": ["c1", "c2"],
            "customer_state": ["SP", "RJ"],
            "customer_city": ["Sao Paulo", "Rio"],
            "customer_segment": ["consumer", "consumer"],
            "account_creation_date": ["2026-01-01 00:00:00", "2026-01-01 00:00:00"],
        }
    )


@pytest.fixture
def valid_products_df():
    return pl.DataFrame(
        {
            "product_id": ["p1", "p2"],
            "product_category_name": ["tech", "home"],
            "product_length_cm": [10.0, 20.0],
            "product_height_cm": [5.0, 10.0],
            "product_width_cm": [15.0, 30.0],
            "product_fragility_index": ["low", "low"],
            "product_weight_g": [500.0, 1000.0],
            "supplier_tier": ["gold", "silver"],
        }
    )


# =============================================================================
# LOGGING TESTS
# =============================================================================


def test_init_report():
    report = init_report()
    assert report["status"] == "success"
    assert report["errors"] == []
    assert report["warnings"] == []
    assert report["info"] == []


def test_log_functions(empty_report):
    log_info("info msg", empty_report)
    log_warning("warn msg", empty_report)
    log_error("error msg", empty_report)

    assert "info msg" in empty_report["info"]
    assert "warn msg" in empty_report["warnings"]
    assert "error msg" in empty_report["errors"]


# =============================================================================
# BASE VALIDATION TESTS
# =============================================================================


def test_run_base_validations_success(valid_customers_df, empty_report):
    ok = run_base_validations(
        df=valid_customers_df,
        table_name="df_customers",
        primary_key=["customer_id"],
        required_column=["customer_id", "customer_state"],
        non_nullable_column=["customer_id"],
        report=empty_report,
    )
    assert ok is True
    assert not empty_report["errors"]


def test_run_base_validations_empty_df(empty_report):
    df = pl.DataFrame()
    ok = run_base_validations(df, "test", ["id"], ["id"], ["id"], empty_report)
    assert ok is False
    assert any("dataset is empty" in e for e in empty_report["errors"])


def test_run_base_validations_missing_column(valid_customers_df, empty_report):
    df = valid_customers_df.drop(["customer_state"])
    ok = run_base_validations(
        df,
        "df_customers",
        ["customer_id"],
        ["customer_id", "customer_state"],
        [],
        empty_report,
    )
    assert ok is False
    assert any("missing required columns" in e for e in empty_report["errors"])


def test_run_base_validations_duplicate_pk(empty_report):
    df = pl.DataFrame({"id": ["1", "1"], "val": ["a", "b"]})
    ok = run_base_validations(df, "test", ["id"], ["id", "val"], ["id"], empty_report)
    assert ok is False
    assert any("conflicting duplicate primary key" in e for e in empty_report["errors"])


def test_run_base_validations_repairable_duplicate(empty_report):
    df = pl.DataFrame({"id": ["1", "1"], "val": ["a", "a"]})
    ok = run_base_validations(df, "test", ["id"], ["id", "val"], ["id"], empty_report)
    assert ok is True
    assert any("eligible for deduplication" in w for w in empty_report["warnings"])


def test_run_base_validations_null_pk(empty_report):
    df = pl.DataFrame({"id": [None, "2"], "val": ["a", "b"]})
    ok = run_base_validations(df, "test", ["id"], ["id", "val"], [], empty_report)
    assert ok is True
    assert any(
        "rows with null primary key values" in w for w in empty_report["warnings"]
    )


# =============================================================================
# EVENT FACT VALIDATION TESTS
# =============================================================================


def test_run_event_fact_validations_success(valid_orders_df, empty_report):
    ok = run_event_fact_validations(valid_orders_df, "df_orders", empty_report)
    assert ok is True
    assert not empty_report["warnings"]


def test_run_event_fact_validations_temporal_error(valid_orders_df, empty_report):
    # Approval before purchase
    valid_orders_df = valid_orders_df.with_columns(
        pl.when(pl.col("order_id") == "o1")
        .then(pl.lit("2026-03-24 10:00:00"))
        .otherwise(pl.col("order_approved_at"))
        .alias("order_approved_at")
    )
    ok = run_event_fact_validations(valid_orders_df, "df_orders", empty_report)
    assert ok is True
    assert any("approval precedes purchase" in w for w in empty_report["warnings"])


def test_run_event_fact_validations_unparsable_ts(valid_orders_df, empty_report):
    valid_orders_df = valid_orders_df.with_columns(
        pl.when(pl.col("order_id") == "o1")
        .then(pl.lit("garbage"))
        .otherwise(pl.col("order_purchase_timestamp"))
        .alias("order_purchase_timestamp")
    )
    ok = run_event_fact_validations(valid_orders_df, "df_orders", empty_report)
    assert ok is True
    assert any("unparsable timestamp values" in w for w in empty_report["warnings"])


# =============================================================================
# TRANSACTION DETAIL VALIDATION TESTS
# =============================================================================


def test_run_transaction_detail_validations_negative(empty_report):
    df = pl.DataFrame({"order_id": ["o1"], "price": [-10.0]})
    ok = run_transaction_detail_validations(df, "test", empty_report)
    assert ok is True
    assert any("negative values in numeric column" in e for e in empty_report["errors"])


# =============================================================================
# CROSS-TABLE VALIDATION TESTS
# =============================================================================


def test_run_cross_table_validations_orphans(empty_report):
    orders = pl.DataFrame({"order_id": ["o1"]})
    items = pl.DataFrame({"order_id": ["o1", "o2"]})  # o2 is orphan
    payments = pl.DataFrame({"order_id": ["o3"]})  # o3 is orphan

    tables = {"df_orders": orders, "df_order_items": items, "df_payments": payments}
    ok = run_cross_table_validations(tables, empty_report)
    assert ok is True
    assert any("orphan records" in w for w in empty_report["warnings"])


# =============================================================================
# APPLY VALIDATION (EXECUTOR) TESTS
# =============================================================================


def test_apply_validation_integration(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_payments_df,
    valid_customers_df,
    valid_products_df,
):
    run_context = RunContext.create(base=tmp_path)
    run_context.initialize_directories()

    # Create date-suffixed files for loader
    suffix = "2026_03_25"
    valid_orders_df.write_csv(run_context.raw_snapshot_path / f"df_orders_{suffix}.csv")
    valid_order_items_df.write_csv(
        run_context.raw_snapshot_path / f"df_order_items_{suffix}.csv"
    )
    valid_payments_df.write_csv(
        run_context.raw_snapshot_path / f"df_payments_{suffix}.csv"
    )
    valid_customers_df.write_csv(
        run_context.raw_snapshot_path / f"df_customers_{suffix}.csv"
    )
    valid_products_df.write_csv(
        run_context.raw_snapshot_path / f"df_products_{suffix}.csv"
    )

    report = apply_validation(run_context)
    assert report["status"] == "success"
    assert not report["errors"]


def test_apply_validation_missing_table(tmp_path):
    run_context = RunContext.create(base=tmp_path)
    run_context.initialize_directories()

    # No files created
    with pytest.raises(FileNotFoundError):
        apply_validation(run_context)
