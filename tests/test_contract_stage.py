# =============================================================================
# UNIT TESTS FOR contract_logic.py and contract_executor.py
# =============================================================================

import polars as pl
import pytest
from data_pipeline.shared.run_context import RunContext
from data_pipeline.contract.contract_executor import apply_contract
from data_pipeline.contract.contract_logic import (
    deduplicate_exact_events,
    remove_unparsable_timestamps,
    remove_impossible_timestamps,
    remove_rows_with_null_constraint,
    cascade_drop_by_order_id,
    enforce_parent_reference,
    enforce_schema,
)
from data_pipeline.contract.id_registrar import (
    discover_uuids,
    lookup_mapping_storage,
    generate_and_persist_delta,
    extract_entity_mappings,
)

# ------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------


@pytest.fixture
def sample_orders_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "customer_id": ["c1", "c2", "c3"],
            "order_status": ["delivered", "delivered", "delivered"],
            "order_purchase_timestamp": [
                "2026-03-25 10:00:00",
                "2026-03-25 11:00:00",
                "2026-03-25 12:00:00",
            ],
            "order_approved_at": [
                "2026-03-25 10:05:00",
                "2026-03-25 11:05:00",
                "2026-03-25 12:05:00",
            ],
            "order_delivered_timestamp": [
                "2026-03-27 10:00:00",
                "2026-03-27 11:00:00",
                "2026-03-27 12:00:00",
            ],
            "order_estimated_delivery_date": ["2026-03-28", "2026-03-28", "2026-03-28"],
        }
    )


@pytest.fixture
def sample_payments_df():
    return pl.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "payment_sequential": [1, 1, 1],
            "payment_value": [100.0, 200.0, 300.0],
        }
    )


# ------------------------------------------------------------
# LOGIC UNIT TESTS
# ------------------------------------------------------------


def test_deduplicate_exact_events():
    df = pl.DataFrame({"a": [1, 1, 2], "b": [2, 2, 3]})
    filtered, removed = deduplicate_exact_events(df)
    assert len(filtered) == 2
    assert removed == 1


def test_remove_unparsable_timestamps():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_purchase_timestamp": ["2026-01-01 10:00:00", "garbage"],
            "order_approved_at": ["2026-01-01 10:05:00", "2026-01-01 10:05:00"],
            "order_delivered_timestamp": ["2026-01-02 10:00:00", "2026-01-02 10:00:00"],
            "order_estimated_delivery_date": ["2026-01-03", "2026-01-03"],
        }
    )
    filtered, removed, inv_ids = remove_unparsable_timestamps(df)
    assert len(filtered) == 1
    assert removed == 1
    assert "o2" in inv_ids


def test_remove_impossible_timestamps():
    # Delivered before purchase
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "order_purchase_timestamp": ["2026-03-25 10:00:00"],
            "order_approved_at": ["2026-03-25 10:05:00"],
            "order_delivered_timestamp": ["2026-03-24 10:00:00"],
        }
    )
    filtered, removed, inv_ids = remove_impossible_timestamps(df)
    assert len(filtered) == 0
    assert removed == 1
    assert "o1" in inv_ids


def test_cascade_drop_by_order_id():
    df = pl.DataFrame({"order_id": ["o1", "o2", "o3"]})
    invalid = {"o1", "o3"}

    filtered, removed = cascade_drop_by_order_id(df, invalid)

    assert len(filtered) == 1
    assert removed == 2
    assert filtered[0, "order_id"] == "o2"


def test_enforce_parent_reference():
    df = pl.DataFrame({"order_id": ["o1", "o2", "ghost"]})
    valid = {"o1", "o2"}

    filtered, removed = enforce_parent_reference(df, valid)

    assert len(filtered) == 2
    assert removed == 1
    assert "ghost" not in filtered["order_id"].to_list()


def test_remove_rows_with_null_constraint():
    df = pl.DataFrame({"order_id": ["o1", "o2", None, "o4"]})
    non_nullable = ["order_id"]

    filtered, removed, invalid_ids = remove_rows_with_null_constraint(df, non_nullable)

    assert len(filtered) == 3
    assert len(invalid_ids) == 1
    assert removed == 1


def test_enforce_schema():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "customer_id": ["c1", "c2", "c3"],
            "extra_col": [1, 2, 3],
            "state": ["ST", "NT", "NK"],
        }
    )
    req_col = ["order_id", "customer_id", "state"]
    dtype = {"order_id": pl.String, "customer_id": pl.String, "state": pl.Categorical}

    filtered, removed = enforce_schema(df, req_col, dtype)

    assert len(filtered) == 3
    assert removed == 1
    assert filtered["order_id"].dtype == pl.String
    assert filtered["state"].dtype == pl.Categorical


# ------------------------------------------------------------
# ID REGISTRAR UNIT TESTS
# ------------------------------------------------------------


def test_discover_uuids_mixed_formats(tmp_path):
    # Setup raw files (CSV and Parquet)
    raw_path = tmp_path / "raw"
    raw_path.mkdir()

    # Table 1: Parquet
    df1 = pl.DataFrame({"order_id": ["o1", "o2"]})
    df1.write_parquet(raw_path / "df_orders_2026_04_01.parquet")

    # Table 2: CSV
    df2 = pl.DataFrame({"order_id": ["o2", "o3"]})
    df2.write_csv(raw_path / "df_order_items_2026_04_01.csv")

    tables = ["df_orders", "df_order_items"]
    uuids = discover_uuids(raw_path, tables, "order_id")

    assert uuids.len() == 3
    assert set(uuids.to_list()) == {"o1", "o2", "o3"}


def test_lookup_mapping_storage_uniqueness(tmp_path):
    storage_dir = tmp_path / "storage" / "order_id"
    storage_dir.mkdir(parents=True)

    # Create history with a duplicate UUID across different deltas
    pl.DataFrame({"order_id": ["o1"], "order_id_int": [1]}).write_parquet(
        storage_dir / "d1.parquet"
    )
    pl.DataFrame({"order_id": ["o1"], "order_id_int": [1]}).write_parquet(
        storage_dir / "d2.parquet"
    )

    storage_glob = str(storage_dir / "*.parquet")
    batch_uuids = pl.Series("order_id", ["o1"])

    known_df, max_id = lookup_mapping_storage(storage_glob, "order_id", batch_uuids)

    assert known_df.height == 1  # Uniqueness check
    assert max_id == 1


def test_generate_and_persist_delta(tmp_path):
    runtime_dir = tmp_path / "runtime"
    missing = pl.Series("order_id", ["o10", "o11"])

    new_df = generate_and_persist_delta(missing, 5, "order_id", runtime_dir, "run123")

    assert new_df.height == 2
    assert new_df["order_id_int"].to_list() == [6, 7]
    assert (runtime_dir / "order_id" / "map_run123.parquet").exists()


def test_extract_entity_mappings_orchestration(tmp_path, monkeypatch):
    run_context = RunContext.create(base=tmp_path, storage=tmp_path / "storage")
    run_context.initialize_directories()

    # Mock all raw data files with required columns to satisfy ID_ENTITY_MAP
    raw_path = run_context.raw_snapshot_path

    pl.DataFrame({"order_id": ["o1"], "customer_id": ["c1"]}).write_parquet(
        raw_path / "df_orders_2026.parquet"
    )

    pl.DataFrame(
        {"order_id": ["o1"], "product_id": ["p1"], "seller_id": ["s1"]}
    ).write_parquet(raw_path / "df_order_items_2026.parquet")

    pl.DataFrame({"customer_id": ["c1"]}).write_parquet(
        raw_path / "df_customers_2026.parquet"
    )

    pl.DataFrame({"order_id": ["o1"]}).write_parquet(
        raw_path / "df_payments_2026.parquet"
    )

    # Mock products just to be safe though not strictly in ID_ENTITY_MAP as a source
    pl.DataFrame({"product_id": ["p1"]}).write_parquet(
        raw_path / "df_products_2026.parquet"
    )

    # Mock promote to avoid GCS errors in local test
    monkeypatch.setattr(
        "data_pipeline.contract.id_registrar.promote_new_mapping_files", lambda *_: None
    )

    mappings = extract_entity_mappings(run_context)

    assert "order_id" in mappings
    assert "customer_id" in mappings
    assert "product_id" in mappings
    assert "seller_id" in mappings

    # Verify one result
    result = mappings["order_id"].collect()
    assert "order_id_int" in result.columns
    assert result[0, "order_id_int"] == 1


# ------------------------------------------------------------
# EXECUTOR INTEGRATION TESTS
# ------------------------------------------------------------


def test_apply_contract_orders_success(tmp_path, sample_orders_df):
    run_context = RunContext.create(base=tmp_path, storage=tmp_path / "storage")
    run_context.initialize_directories()

    suffix = "2026_03_25"
    sample_orders_df.write_csv(
        run_context.raw_snapshot_path / f"df_orders_{suffix}.csv"
    )

    # Mock Discovery Mappings
    master_mappings = {
        "order_id": pl.DataFrame(
            {"order_id": ["o1", "o2", "o3"], "order_id_int": [1, 2, 3]}
        ).lazy(),
        "customer_id": pl.DataFrame(
            {"customer_id": ["c1", "c2", "c3"], "customer_id_int": [1, 2, 3]}
        ).lazy(),
    }

    report, inv_ids, val_ids = apply_contract(
        run_context, "df_orders", master_mappings=master_mappings
    )

    assert report["status"] == "success"
    assert report["final_rows"] == 3
    assert len(val_ids) == 3

    # Check that integer columns are present
    df_result = pl.read_parquet(
        run_context.contracted_path / f"df_orders_{suffix}.parquet"
    )
    assert "order_id_int" in df_result.columns
    assert "customer_id_int" in df_result.columns


def test_apply_contract_cascade_and_valid_propagation(
    tmp_path, sample_orders_df, sample_payments_df
):
    run_context = RunContext.create(base=tmp_path, storage=tmp_path / "storage")
    run_context.initialize_directories()

    sample_orders_df = sample_orders_df.with_columns(
        pl.when(pl.col("order_id") == "o2")
        .then(pl.lit("garbage"))
        .otherwise(pl.col("order_purchase_timestamp"))
        .alias("order_purchase_timestamp"),
        pl.when(pl.col("order_id") == "o3")
        .then(pl.lit("2026-01-01 00:00:00"))
        .otherwise(pl.col("order_delivered_timestamp"))
        .alias("order_delivered_timestamp"),
    )

    suffix = "2026_03_25"
    sample_orders_df.write_csv(
        run_context.raw_snapshot_path / f"df_orders_{suffix}.csv"
    )
    sample_payments_df.write_csv(
        run_context.raw_snapshot_path / f"df_payments_{suffix}.csv"
    )

    # Mock Discovery Mappings
    master_mappings = {
        "order_id": pl.DataFrame(
            {"order_id": ["o1", "o2", "o3"], "order_id_int": [1, 2, 3]}
        ).lazy(),
        "customer_id": pl.DataFrame(
            {"customer_id": ["c1", "c2", "c3"], "customer_id_int": [1, 2, 3]}
        ).lazy(),
    }

    rep_o, inv_o, val_o = apply_contract(
        run_context, "df_orders", master_mappings=master_mappings
    )
    assert "o2" in inv_o  # unparsable
    assert "o3" in inv_o  # impossible
    assert "o1" in val_o  # only one valid

    rep_p, inv_p, val_p = apply_contract(
        run_context,
        "df_payments",
        master_mappings=master_mappings,
        invalid_order_ids=inv_o,
        valid_order_ids=val_o,
    )

    assert rep_p["removed_cascade_rows"] == 2  # o2 and o3 dropped
    assert rep_p["final_rows"] == 1


def test_apply_contract_unknown_table(tmp_path):
    run_context = RunContext.create(base=tmp_path)
    run_context.initialize_directories()

    report, inv, val = apply_contract(run_context, "non_existent", master_mappings={})
    assert report["status"] == "failed"
    assert "Unknown table" in report["errors"][0]
