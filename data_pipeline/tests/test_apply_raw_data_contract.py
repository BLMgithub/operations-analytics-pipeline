# =============================================================================
# UNIT TESTS FOR apply_raw_data_contract.py
# =============================================================================

import pandas as pd
import pytest
from shutil import copytree

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.apply_raw_data_contract import (
    deduplicate_exact_events,
    remove_unparsable_timestamps,
    remove_impossible_timestamps,
    apply_contract,
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------


@pytest.fixture
def valid_orders_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_purchase_timestamp": ["2023-01-01", "2023-01-02"],
            "order_approved_at": ["2023-01-01", "2023-01-02"],
            "order_delivered_timestamp": ["2023-01-03", "2023-01-04"],
            "order_estimated_delivery_date": ["2023-01-05", "2023-01-06"],
        }
    )


@pytest.fixture
def invalid_order_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_purchase_timestamp": ["2023-01-02", "invalid"],
            "order_approved_at": ["2023-01-01", "invalid"],
            "order_delivered_timestamp": ["2023-01-03", "2023-01-04"],
            "order_estimated_delivery_date": ["2023-01-05", "2023-01-06"],
        }
    )


@pytest.fixture
def invalid_temporal_order_df():
    return pd.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_purchase_timestamp": ["2023-01-01", "2023-01-02"],
            "order_approved_at": ["2022-01-01", "2023-01-02"],
            "order_delivered_timestamp": ["2023-01-03", "2023-01-04"],
            "order_estimated_delivery_date": ["2023-01-05", "2023-01-06"],
        }
    )


# ------------------------------------------------------------
# DEDUPLICATION CONTRACT
# ------------------------------------------------------------


def test_deduplicate_exact_events_passed():
    df = pd.DataFrame({"id": ["x", "x"], "value": [1, 2]})

    result, removed_count = deduplicate_exact_events(df)

    assert len(result) == 2
    assert removed_count == 0
    assert result.iloc[0]["id"] == "x"
    assert result.iloc[0]["value"] == 1
    assert result.iloc[1]["id"] == "x"
    assert result.iloc[1]["value"] == 2


def test_deduplicate_exact_events_removes_duplicates():
    df = pd.DataFrame({"id": ["x", "x"], "value": [1, 1]})

    result, removed_count = deduplicate_exact_events(df)

    assert len(result) == 1
    assert removed_count == 1
    assert result.iloc[0]["id"] == "x"
    assert result.iloc[0]["value"] == 1


# ------------------------------------------------------------
# UNSPARSABLE TIMESTAMPS CONTRACT
# ------------------------------------------------------------


def test_remove_unparsable_timestamps_passed(valid_orders_df):

    initial_count = len(valid_orders_df)
    result, remove_count, invalid_ids = remove_unparsable_timestamps(valid_orders_df)

    assert len(result) == initial_count
    assert remove_count == 0 or invalid_ids == {}


def test_remove_unparsable_timestamps_drops_invalid_rows(invalid_order_df):

    initial_count = len(invalid_order_df)
    result, remove_count, invalid_ids = remove_unparsable_timestamps(invalid_order_df)

    assert len(result) < initial_count
    assert remove_count == 1
    assert "o2" in invalid_ids


# ------------------------------------------------------------
# IMPOSSIBLE TIMESTAMPS ORDER
# ------------------------------------------------------------


def test_remove_impossible_timestamps(valid_orders_df):

    initial_count = len(valid_orders_df)
    result, remove_count, invalid_ids = remove_impossible_timestamps(valid_orders_df)

    assert len(result) == initial_count
    assert remove_count == 0 or invalid_ids == {}


def test_remove_impossible_timestamps_drops_invalid_rows(invalid_temporal_order_df):

    initial_count = len(invalid_temporal_order_df)
    result, remove_count, invalid_ids = remove_impossible_timestamps(
        invalid_temporal_order_df
    )

    assert len(result) < initial_count
    assert remove_count == 1
    assert "o1" in invalid_ids


# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------


def test_apply_contract_event_fact_success(tmp_path):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "order_purchase_timestamp": [
                "2026-01-01",
                "2026-01-01",  # duplicate
                "bad_timestamp",  # unparsable
                "2026-01-03",
            ],
            "order_approved_at": [
                "2026-01-01",
                "2026-01-01",
                "2026-01-02",
                "2025-12-01",  # impossible (before purchase)
            ],
            "order_delivered_timestamp": [
                "2026-01-05",
                "2026-01-05",
                "2026-01-06",
                "2026-01-02",
            ],
            "order_estimated_delivery_date": [
                "2026-01-06",
                "2026-01-06",
                "2026-01-07",
                "2026-01-04",
            ],
        }
    )

    df.to_csv(raw_dir / "df_orders_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report, _ = apply_contract(run_context, "df_orders")

    assert report["deduplicated_rows"] == 0
    assert report["removed_unparsable_timestamps"] == 1
    assert report["removed_impossible_timestamps"] == 1
    assert report["final_rows"] == 2

    output_file = run_context.contracted_path / "df_orders_contracted.parquet"

    assert output_file.exists()


def test_apply_contract_unknown_table(tmp_path):

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    report, _ = apply_contract(run_context, "unknown_table")

    assert report["status"] == "failed"
    assert "Unknown table" in report["errors"][0]


def test_apply_contract_duplicate_on_entity_reference(tmp_path):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    df = pd.DataFrame(
        {
            "customer_id": [1, 1, 3],  # 1 exact duplicate
            "customer_zip_code_prefix": ["zip1", "zip1", "zip3"],
            "customer_city": ["city1", "city1", "city3"],
            "customer_state": ["state1", "state1", "state3"],
        }
    )

    df.to_csv(raw_dir / "df_customers_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report, _ = apply_contract(run_context, "df_customers")

    assert report["status"] == "success"
    assert report["deduplicated_rows"] == 1
    assert report["final_rows"] == 2


def test_apply_contract_duplicate_on_transactional_detail(tmp_path):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    df = pd.DataFrame(
        {
            "order_id": [
                "o1",
                "o1",
                "o2",
                "o3",
            ],  # No duplicates due to pk order_id and payment_sequential
            "payment_sequential": [1, 1, 1, 3],
            "payment_type": ["credit", "credit", "credit", "cash"],
            "payment_installments": [1, 2, 3, 4],
            "payment_value": [100.1, 50.2, 25.3, 12.5],
        }
    )

    df.to_csv(raw_dir / "df_payments_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    report, _ = apply_contract(run_context, "df_payments")

    assert report["status"] == "success"
    assert report["deduplicated_rows"] == 0
    assert report["final_rows"] == 4


def test_apply_contract_cascade_drop_with_order_id(tmp_path):

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    df_order = pd.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "order_purchase_timestamp": [
                "invalid",  # unparsable
                "2023-01-02",
                "2023-01-03",
            ],
            "order_approved_at": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "order_delivered_timestamp": ["2023-01-01", "2023-01-04", "2023-01-05"],
            "order_estimated_delivery_date": ["2023-01-01", "2023-01-06", "2023-01-07"],
        }
    )
    df_payments = pd.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "payment_sequential": [1, 1, 3],
            "payment_type": ["credit", "credit", "cash"],
            "payment_installments": [1, 2, 3],
            "payment_value": [100.1, 50.2, 25.3],
        }
    )
    df_order_items = pd.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "product_id": ["prod1", "prod2", "prod5"],
            "seller_id": ["seller1", "seller2", "seller5"],
            "price": [12.3, 45.6, 78.9],
            "shipping_charges": [1.23, 34.5, 678],
        }
    )

    df_order.to_csv(raw_dir / "df_orders_2026_01.csv", index=False)
    df_payments.to_csv(raw_dir / "df_payments_2026_01.csv", index=False)
    df_order_items.to_csv(raw_dir / "df_order_items_2026_01.csv", index=False)

    run_context = RunContext.create(base_path=tmp_path)
    run_context.initialize_directories()

    copytree(raw_dir, run_context.raw_snapshot_path, dirs_exist_ok=True)

    invalid_ids = set()

    # Get order_ids of unparsable timestamps row
    report_df_order, new_invalid = apply_contract(run_context, "df_orders", invalid_ids)

    # Accumulate order_ids
    invalid_ids |= new_invalid

    # Apply cascade_drop_by_order_id with accumulated order_ids
    report_df_payments, new_invalid = apply_contract(
        run_context, "df_payments", invalid_ids
    )
    report_df_order_items, new_invalid = apply_contract(
        run_context, "df_order_items", invalid_ids
    )

    assert "o1" in invalid_ids
    assert report_df_order["removed_unparsable_timestamps"] == 1
    assert report_df_order["final_rows"] == 2

    assert report_df_payments["removed_cascade_rows"] == 1
    assert report_df_payments["final_rows"] == 2

    assert report_df_order_items["removed_cascade_rows"] == 1
    assert report_df_order_items["final_rows"] == 2


# =============================================================================
# UNIT TESTS END
# =============================================================================
