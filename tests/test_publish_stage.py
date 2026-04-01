# =============================================================================
# UNIT TESTS FOR publish_lifecycle.py
# =============================================================================

import polars as pl
import pytest
import json
from pathlib import Path

from data_pipeline.shared.run_context import RunContext
from data_pipeline.semantic.registry import SEMANTIC_MODULES
from data_pipeline.publish.publish_executor import execute_publish_lifecycle
from data_pipeline.publish.publish_logic import (
    init_report,
    log_info,
    log_error,
    run_integrity_gate,
    promote_semantic_version,
    activate_published_version,
)
from data_pipeline.shared.modeling_configs import (
    SELLER_FACT_SCHEMA,
    SELLER_DIM_SCHEMA,
    CUSTOMER_FACT_SCHEMA,
    CUSTOMER_DIM_SCHEMA,
    PRODUCT_FACT_SCHEMA,
    PRODUCT_DIM_SCHEMA,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_seller_fact():
    return pl.DataFrame({col: ["val"] for col in SELLER_FACT_SCHEMA})


@pytest.fixture
def valid_seller_dim():
    return pl.DataFrame({col: ["val"] for col in SELLER_DIM_SCHEMA})


@pytest.fixture
def valid_customer_fact():
    return pl.DataFrame({col: ["val"] for col in CUSTOMER_FACT_SCHEMA})


@pytest.fixture
def valid_customer_dim():
    return pl.DataFrame({col: ["val"] for col in CUSTOMER_DIM_SCHEMA})


@pytest.fixture
def valid_product_fact():
    return pl.DataFrame({col: ["val"] for col in PRODUCT_FACT_SCHEMA})


@pytest.fixture
def valid_product_dim():
    return pl.DataFrame({col: ["val"] for col in PRODUCT_DIM_SCHEMA})


# ------------------------------------------------------------
# REPORTING & LOGS
# ------------------------------------------------------------


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


# ------------------------------------------------------------
# PRE-PUBLISH INTEGRITY GATE
# ------------------------------------------------------------


def setup_semantic_files(run_context, df_map):
    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    for module_name, module in SEMANTIC_MODULES.items():
        module_path = run_context.semantic_path / module_name
        module_path.mkdir(parents=True, exist_ok=True)
        for table_name in module["tables"]:
            df = df_map[table_name]
            filename = f"{table_name}_{year}_{month}_{day}.parquet"
            # df is now pl.DataFrame
            df.write_parquet(module_path / filename)


def test_run_integrity_gate_success(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
    valid_customer_fact,
    valid_customer_dim,
    valid_product_fact,
    valid_product_dim,
):
    run_id = "20230101T120000"
    run_context = RunContext.create(base=tmp_path, storage=tmp_path, run_id=run_id)
    run_context.initialize_directories()

    df_map = {
        "seller_weekly_fact": valid_seller_fact,
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    setup_semantic_files(run_context, df_map)
    report = run_integrity_gate(run_context)
    assert report["status"] == "success"


def test_run_integrity_gate_fails_on_missing_directory(tmp_path):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    # Don't initialize directories or setup files
    report = run_integrity_gate(run_context)
    assert report["status"] == "failed"
    assert "Semantic directory is missing" in report["errors"]


def test_run_integrity_gate_fails_on_semantic_file_mismatch(
    tmp_path,
    valid_seller_fact,
):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    # Only setup one module but incomplete
    module_path = run_context.semantic_path / "seller_semantic"
    module_path.mkdir(parents=True, exist_ok=True)
    valid_seller_fact.write_parquet(
        module_path / "seller_weekly_fact_2023_01_01.parquet"
    )

    report = run_integrity_gate(run_context)
    assert report["status"] == "failed"


def test_run_integrity_gate_fails_on_empty_dataframe(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
    valid_customer_fact,
    valid_customer_dim,
    valid_product_fact,
    valid_product_dim,
):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    df_map = {
        "seller_weekly_fact": pl.DataFrame(),  # Empty
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    setup_semantic_files(run_context, df_map)
    report = run_integrity_gate(run_context)
    assert report["status"] == "failed"
    # Current implementation fails on missing columns if dataframe is empty
    assert any("required column(s)" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_missing_columns(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
    valid_customer_fact,
    valid_customer_dim,
    valid_product_fact,
    valid_product_dim,
):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    # Drop a column using Polars
    df_map = {
        "seller_weekly_fact": valid_seller_fact.drop(valid_seller_fact.columns[0]),
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    setup_semantic_files(run_context, df_map)
    report = run_integrity_gate(run_context)
    assert report["status"] == "failed"
    assert any("required column(s)" in error for error in report["errors"])


# ------------------------------------------------------------
# PROMOTE VALIDATED SEMANTIC
# ------------------------------------------------------------


def test_promote_semantic_version_success(tmp_path):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    run_context.semantic_path.mkdir(parents=True, exist_ok=True)

    report = promote_semantic_version(run_context)
    assert report["status"] == "success"
    assert Path(run_context.version_path).exists()


def test_promote_semantic_version_fails_on_existing_version_directory(tmp_path):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()
    Path(run_context.version_path).mkdir(parents=True)

    report = promote_semantic_version(run_context)
    assert report["status"] == "failed"
    assert "Version directory already exists" in report["errors"]


# ------------------------------------------------------------
# ACTIVATE VERSION
# ------------------------------------------------------------


def test_activate_published_version_success(tmp_path):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    report = activate_published_version(run_context)
    assert report["status"] == "success"
    assert Path(run_context.latest_pointer_path).exists()

    with open(run_context.latest_pointer_path, "r") as f:
        data = json.load(f)
        assert data["run_id"] == "20230101T120000"


# ------------------------------------------------------------
# EXECUTE PUBLISH LIFECYCLE
# ------------------------------------------------------------


def test_execute_publish_lifecycle_success(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
    valid_customer_fact,
    valid_customer_dim,
    valid_product_fact,
    valid_product_dim,
):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()

    df_map = {
        "seller_weekly_fact": valid_seller_fact,
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    setup_semantic_files(run_context, df_map)

    report = execute_publish_lifecycle(run_context)
    assert report["status"] == "success"
    assert Path(run_context.version_path).exists()
    assert Path(run_context.latest_pointer_path).exists()


def test_execute_publish_lifecycle_fails_on_gate(tmp_path):
    run_context = RunContext.create(
        base=tmp_path, storage=tmp_path, run_id="20230101T120000"
    )
    run_context.initialize_directories()
    # No files setup -> Gate fails

    report = execute_publish_lifecycle(run_context)
    assert report["status"] == "failed"
    assert report["failed_step"] == "integrity_gate"
