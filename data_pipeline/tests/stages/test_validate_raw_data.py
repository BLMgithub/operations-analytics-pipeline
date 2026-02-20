# =============================================================================
# UNIT TESTS FOR validate_raw_data.py
# =============================================================================

import pandas as pd
import pytest

from shutil import copytree
from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.validate_raw_data import (
    init_report,
    log_info,
    log_warning,
    log_error,
    run_base_validations,
    run_event_fact_validations,
    run_transaction_detail_validations,
    run_cross_table_validations,
    apply_validation,
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_orders_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["customer1", "customer2"],
            "order_status": ["success", "fail"],
            "order_purchase_timestamp": ["2023-01-01 01:05:10", "2023-01-02 05:25:30"],
            "order_approved_at": ["2023-01-01 02:10:50", "2023-01-02 06:55:15"],
            "order_delivered_timestamp": ["2023-01-03 10:15:20", "2023-01-04 10:25:35"],
            "order_estimated_delivery_date": ["2023-01-05", "2023-01-06"],
        }
    )


@pytest.fixture
def valid_transaction_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "payment_sequential": [1, 1],
            "payment_type": ["credit", "credit"],
            "payment_installments": [1, 2],
            "payment_value": [123.4, 56.78],
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
            "shipping_charges": [1.23, 45.6],
        }
    )


@pytest.fixture
def valid_customers_df():
    return pd.DataFrame(
        {
            "customer_id": [1, 2],
            "customer_zip_code_prefix": [
                "zip1",
                "zip2",
            ],
            "customer_city": ["city1", "city2"],
            "customer_state": ["state1", "state2"],
        }
    )


@pytest.fixture
def valid_products_df():
    return pd.DataFrame(
        {
            "product_id": ["prod1", "prod2"],
            "product_category_name": ["categ1", "categ2"],
            "product_weight_g": [491, 500],
            "product_length_cm": [19.0, 20.0],
            "product_height_cm": [12.0, 13.0],
            "product_width_cm": [16.0, 15.0],
        }
    )


# ------------------------------------------------------------
# VALIDATION REPORT & LOGS
# ------------------------------------------------------------


def test_init_report_structure():
    report = init_report()

    assert set(report.keys()) == {"errors", "warnings", "info"}
    assert all(isinstance(v, list) for v in report.values())


def test_log_error_appends_only_to_errors(empty_report):
    log_error("errors", empty_report)

    assert empty_report["errors"] == ["errors"]


def test_log_warning_appends_only_to_warnings(empty_report):
    log_warning("warnings", empty_report)

    assert empty_report["warnings"] == ["warnings"]


def test_log_info_appends_only_to_info(empty_report):
    log_info("info", empty_report)

    assert empty_report["info"] == ["info"]


# ------------------------------------------------------------
# BASE VALIDATIONS (ALL TABLE)
# ------------------------------------------------------------


def test_base_validation_fails_on_missing_allowed_column(
    empty_report, valid_customers_df
):
    df = valid_customers_df.drop(columns="customer_city")
    ok = run_base_validations(
        df,
        "df_customers",
        ["customer_id"],
        [
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        empty_report,
    )

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_customers: missing allowed column(s): ['customer_city']" in error
        for error in empty_report["errors"]
    )


def test_base_validation_fails_on_invalid_extra_column(
    empty_report, valid_customers_df
):

    df = valid_customers_df
    df["extra_column"] = ["extra", "extra"]
    ok = run_base_validations(
        df,
        "df_customers",
        ["customer_id"],
        [
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        empty_report,
    )

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_customers: non-allowed extra column(s): ['extra_column']" in error
        for error in empty_report["errors"]
    )


def test_base_validation_fails_on_empty_df(empty_report):

    df = pd.DataFrame()
    ok = run_base_validations(df, "df_test", ["id"], ["col"], empty_report)

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any("df_test: dataset is empty" in error for error in empty_report["errors"])


def test_base_validation_fails_on_missing_pk(empty_report):

    df = pd.DataFrame({"x": [1, 2]})
    ok = run_base_validations(df, "df_test", ["id"], ["x"], empty_report)

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_test: missing primary key column(s): ['id']" in error
        for error in empty_report["errors"]
    )


def test_base_validation_logs_error_on_conflicting_duplicate_pk(
    empty_report, valid_products_df
):

    df = valid_products_df
    df.loc[df["product_id"] == "prod2", "product_id"] = "prod1"
    ok = run_base_validations(
        df,
        "df_products",
        ["product_id"],
        [
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        empty_report,
    )

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_products: conflicting duplicate primary key records detected" in error
        for error in empty_report["errors"]
    )


def test_base_validation_logs_warning_on_exact_duplicate_pk(empty_report):

    df = pd.DataFrame(
        {
            "order_id": ["o1", "o1", "o2"],
            "payment_sequential": [1, 1, 2],
            "payment_type": ["credit", "credit", "cash"],
            "payment_installments": [1, 1, 2],
            "payment_value": [123.4, 123.4, 56.78],
        }
    )

    ok = run_base_validations(
        df,
        "df_payments",
        ["order_id"],
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        empty_report,
    )

    assert ok is True
    assert len(empty_report["warnings"]) == 1
    assert any(
        "1 duplicate row(s) eligible for deduplication" in warning
        for warning in empty_report["warnings"]
    )


def test_base_validation_passes_with_non_fatal_issues(empty_report):
    df = pd.DataFrame(
        {
            "customer_id": [1, None],
            "customer_zip_code_prefix": ["zip1", "zip3"],
            "customer_city": ["city1", "city3"],
            "customer_state": ["state1", "state3"],
        }
    )

    ok = run_base_validations(
        df,
        "df_customers",
        ["customer_id"],
        ["customer_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
        empty_report,
    )

    assert ok is True
    assert len(empty_report["warnings"]) == 1
    assert any(
        "1 row(s) with null primary key values" in warning
        for warning in empty_report["warnings"]
    )


# ------------------------------------------------------------
# EVENT FACT VALIDATIONS
# ------------------------------------------------------------


def test_event_fact_validation_passes(valid_orders_df, empty_report):
    ok = run_event_fact_validations(valid_orders_df, "df_orders", empty_report)

    assert ok is True
    assert empty_report["errors"] or empty_report["warnings"] == []


def test_event_fact_fails_on_missing_timestamp(valid_orders_df, empty_report):
    df = valid_orders_df.drop(columns=["order_approved_at"])

    ok = run_event_fact_validations(df, "df_orders", empty_report)

    assert ok is False
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_orders: missing required timestamp column(s): ['order_approved_at']"
        in error
        for error in empty_report["errors"]
    )


def test_event_fact_logs_warning_on_invalid_temporal_order(
    valid_orders_df, empty_report
):
    valid_orders_df["order_approved_at"] = [
        "2022-12-01 10:05:20",
        "2022-12-01 09:10:11",
    ]

    ok = run_event_fact_validations(valid_orders_df, "df_orders", empty_report)

    assert ok is True
    assert len(empty_report["warnings"]) == 1
    assert any(
        "df_orders: 2 record(s) where approval precedes purchase" in warning
        for warning in empty_report["warnings"]
    )


# ------------------------------------------------------------
# TRANSACTION DETAIL VALIDATIONS
# ------------------------------------------------------------


def test_transaction_detail_passes(valid_transaction_df, empty_report):
    ok = run_transaction_detail_validations(
        valid_transaction_df, "df_payments", empty_report
    )

    assert ok is True
    assert empty_report["errors"] or empty_report["warnings"] == []


def test_transaction_detail_fails_on_negative_value(empty_report):
    df = pd.DataFrame({"order_id": ["o1"], "payment_value": [-10]})

    ok = run_transaction_detail_validations(df, "df_payments", empty_report)

    assert ok is True
    assert len(empty_report["errors"]) == 1
    assert any(
        "df_payments: 1 negative value(s) in numeric column `payment_value`" in error
        for error in empty_report["errors"]
    )


# ------------------------------------------------------------
# CROSS-TABLE VALIDATIONS
# ------------------------------------------------------------


def test_cross_table_validation_passes(
    valid_orders_df, valid_transaction_df, empty_report
):
    tables = {
        "df_orders": valid_orders_df,
        "df_order_items": pd.DataFrame({"order_id": ["o1"]}),
        "df_payments": valid_transaction_df,
    }

    ok = run_cross_table_validations(tables, empty_report)

    assert ok is True
    assert empty_report["errors"] or empty_report["warnings"] == []


def test_cross_table_logs_on_missing_table(empty_report):
    tables = {"df_order_items": pd.DataFrame(), "df_payments": pd.DataFrame()}

    ok = run_cross_table_validations(tables, empty_report)

    assert ok is False
    assert len(empty_report["info"]) == 1
    assert any(
        "Cross-table validation skipped: missing required table(s): ['df_orders']"
        in info
        for info in empty_report["info"]
    )


# ------------------------------------------------------------
# APPLY VALIDATION
# ------------------------------------------------------------


def test_validation_passes(
    tmp_path,
    valid_orders_df,
    valid_transaction_df,
    valid_order_items_df,
    valid_customers_df,
    valid_products_df,
):

    # Dummy raw structure
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Export to snapshot directory
    valid_orders_df.to_csv(raw_dir / "df_orders_2026_01.csv", index=False)
    valid_order_items_df.to_csv(raw_dir / "df_order_items_2026_01.csv", index=False)
    valid_transaction_df.to_csv(raw_dir / "df_payments_2026_01.csv", index=False)
    valid_customers_df.to_csv(raw_dir / "df_customers_2026_01.csv", index=False)
    valid_products_df.to_csv(raw_dir / "df_products_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_validation(run_context)

    assert len(report["errors"]) == 0
    assert report["errors"] or report["warnings"] == []


def test_validation_fails_on_missing_logical_table(
    tmp_path,
    valid_orders_df,
    valid_order_items_df,
    valid_customers_df,
    valid_products_df,
):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Missing df_payments on snapshot
    valid_orders_df.to_csv(raw_dir / "df_orders_2026_01.csv", index=False)
    valid_order_items_df.to_csv(raw_dir / "df_order_items_2026_01.csv", index=False)
    valid_customers_df.to_csv(raw_dir / "df_customers_2026_01.csv", index=False)
    valid_products_df.to_csv(raw_dir / "df_products_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_validation(run_context)

    assert any(
        "df_payments logical table is missing" in error for error in report["errors"]
    )


def test_validation_fails_on_multiple_errors(tmp_path, valid_orders_df):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    df_orders = valid_orders_df

    df_orders.to_csv(raw_dir / "df_orders_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report = apply_validation(run_context)

    assert len(report["errors"]) > 1


# =============================================================================
# UNIT TESTS END
# =============================================================================
