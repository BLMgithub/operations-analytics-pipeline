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
from data_pipeline.contract.id_registrar import map_uuid_to_int, id_mapping

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


def test_map_uuid_to_int_new_file(tmp_path):
    df = pl.DataFrame({"user_id": ["u1", "u2", "u3"]})
    mapping_file = tmp_path / "user_id_mapping.parquet"

    map_uuid_to_int(df, mapping_file, "user_id")

    assert mapping_file.exists()
    mapping_df = pl.read_parquet(mapping_file)
    assert mapping_df.height == 3
    assert "user_id" in mapping_df.columns
    assert "user_id_int" in mapping_df.columns
    assert mapping_df["user_id_int"].to_list() == [1, 2, 3]
    assert mapping_df["user_id_int"].dtype == pl.UInt32


def test_map_uuid_to_int_update_existing(tmp_path):
    mapping_file = tmp_path / "user_id_mapping.parquet"
    initial_df = pl.DataFrame({"user_id": ["u1", "u2"], "user_id_int": [1, 2]}).cast(
        {"user_id_int": pl.UInt32}
    )
    initial_df.write_parquet(mapping_file)

    new_df = pl.DataFrame({"user_id": ["u2", "u3", "u4"]})
    map_uuid_to_int(new_df, mapping_file, "user_id")

    mapping_df = pl.read_parquet(mapping_file).sort("user_id_int")
    assert mapping_df.height == 4
    assert set(mapping_df["user_id"].to_list()) == {"u1", "u2", "u3", "u4"}
    assert mapping_df["user_id_int"].to_list() == [1, 2, 3, 4]


def test_id_mapping_orchestration(tmp_path):
    runtime_dir = tmp_path / "runtime"
    destination = tmp_path / "destination"
    runtime_dir.mkdir()
    destination.mkdir()

    df = pl.DataFrame({"order_id": ["o1", "o2"], "customer_id": ["c1", "c2"]})

    mapping_dict = {"df_orders": ["order_id", "customer_id"]}

    lf_mapped = id_mapping(df, "df_orders", mapping_dict, runtime_dir, destination)
    result_df = lf_mapped.collect()

    assert "order_id_int" in result_df.columns
    assert "customer_id_int" in result_df.columns
    assert result_df.height == 2

    # Check if mapping files were promoted to destination
    assert (destination / "order_id_mapping.parquet").exists()
    assert (destination / "customer_id_mapping.parquet").exists()


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

    report, inv_ids, val_ids = apply_contract(run_context, "df_orders")

    assert report["status"] == "success"
    assert report["final_rows"] == 3
    assert len(val_ids) == 3
    assert not inv_ids
    assert (run_context.contracted_path / f"df_orders_{suffix}.parquet").exists()


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

    rep_o, inv_o, val_o = apply_contract(run_context, "df_orders")
    assert "o2" in inv_o  # unparsable
    assert "o3" in inv_o  # impossible
    assert "o1" in val_o  # only one valid

    rep_p, inv_p, val_p = apply_contract(
        run_context, "df_payments", invalid_order_ids=inv_o, valid_order_ids=val_o
    )

    assert rep_p["removed_cascade_rows"] == 2  # o2 and o3 dropped
    assert rep_p["final_rows"] == 1
    assert "o1" in set(
        pl.read_parquet(run_context.contracted_path / f"df_payments_{suffix}.parquet")[
            "order_id"
        ].to_list()
    )


def test_apply_contract_unknown_table(tmp_path):
    run_context = RunContext.create(base=tmp_path)
    run_context.initialize_directories()

    report, inv, val = apply_contract(run_context, "non_existent")
    assert report["status"] == "failed"
    assert "Unknown table" in report["errors"][0]
