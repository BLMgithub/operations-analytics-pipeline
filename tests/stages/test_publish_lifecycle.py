# =============================================================================
# UNIT TESTS FOR publish_lifecycle.py
# =============================================================================

import pandas as pd
import pytest
import shutil

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.build_bi_semantic_layer import SEMANTIC_MODULES
from data_pipeline.stages.publish_lifecycle import (
    init_report,
    log_info,
    log_error,
    run_integrity_gate,
    promote_semantic_version,
    execute_publish_lifecycle,
)


@pytest.fixture
def empty_report():
    return init_report()


@pytest.fixture
def valid_seller_fact():
    return pd.DataFrame(
        {
            "seller_id": pd.Series(
                ["seller1", "seller2"],
                dtype="string",
            ),
            "order_year_week": pd.Series(
                ["2023-W01", "2023-W02"],
                dtype="string",
            ),
            "week_start_date": pd.Series(
                ["2023-01-02 09:00:00", "2023-01-04 15:00:00"],
                dtype="datetime64[ns]",
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


@pytest.fixture
def valid_customer_fact():
    return pd.DataFrame(
        {
            "customer_id": pd.Series(
                ["customer1", "customer"],
                dtype="string",
            ),
            "order_year_week": pd.Series(
                ["2023-W01", "2023-W02"],
                dtype="string",
            ),
            "week_start_date": pd.Series(
                ["2023-01-02 09:00:00", "2023-01-04 15:00:00"],
                dtype="datetime64[ns]",
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
def valid_customer_dim():
    return pd.DataFrame(
        {
            "customer_id": pd.Series(["customer1", "customer2"], dtype="string"),
            "customer_state": pd.Series(["state1", "state2"], dtype="string"),
            "run_id": pd.Series(["run_1", "run_1"], dtype="string"),
        }
    )


@pytest.fixture
def valid_product_fact():
    return pd.DataFrame(
        {
            "product_id": pd.Series(
                ["product1", "product2"],
                dtype="string",
            ),
            "order_year_week": pd.Series(
                ["2023-W01", "2023-W02"],
                dtype="string",
            ),
            "week_start_date": pd.Series(
                ["2023-01-02 09:00:00", "2023-01-04 15:00:00"],
                dtype="datetime64[ns]",
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
def valid_product_dim():
    return pd.DataFrame(
        {
            "product_id": pd.Series(["prod1", "prod2"], dtype="string"),
            "product_category_name": pd.Series(["categ1", "categ2"], dtype="string"),
            "product_weight_g": pd.Series([491, 500], dtype="float64"),
            "run_id": pd.Series(["run_1", "run_1"], dtype="string"),
        }
    )


# ------------------------------------------------------------
# # REPORTING & LOGS
# ------------------------------------------------------------


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


# ------------------------------------------------------------
# PRE-PUBLISH VALIDATION GATE
# ------------------------------------------------------------


def test_run_integrity_gate_success(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
    valid_customer_fact,
    valid_customer_dim,
    valid_product_fact,
    valid_product_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    # Match expected table names for each module
    df_map = {
        "seller_weekly_fact": valid_seller_fact,
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    for module_name, module in SEMANTIC_MODULES.items():
        module_path = run_context.semantic_path / module_name
        module_path.mkdir(parents=True, exist_ok=True)

        for table_name in module["tables"]:
            df = df_map[table_name]

            df.to_parquet(
                module_path / f"{table_name}_2023_01.parquet",
                index=False,
            )

    report = run_integrity_gate(run_context)

    assert report["status"] == "success"


def test_run_integrity_gate_fails_on_missing_directory(tmp_path):

    run_context = RunContext.create(base_path=tmp_path)
    report = run_integrity_gate(run_context)

    assert report["status"] == "failed"
    assert "Semantic directory is missing" in report["errors"]


def test_run_integrity_gate_fails_on_semantic_file_mismatch(
    tmp_path,
    valid_seller_fact,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        # Missing valid_seller_dim in semantic/

    report = run_integrity_gate(run_context)

    assert report["status"] == "failed"
    assert any("Semantic file set mismatch" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_loading_parquet_files(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_csv(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        valid_seller_dim.to_csv(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

    report = run_integrity_gate(run_context)

    assert report["status"] == "failed"
    assert any("this is not a parquet file" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_empty_dataframe(tmp_path):

    empty_seller_fact = pd.DataFrame()
    empty_seller_dim = pd.DataFrame()

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        empty_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        empty_seller_dim.to_parquet(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

    report = run_integrity_gate(run_context)

    assert report["status"] == "failed"
    assert any("logical table missing or empty" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_missing_columns(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact = valid_seller_fact.drop(columns="seller_id")

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        valid_seller_dim.to_parquet(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

    report = run_integrity_gate(run_context)

    assert report["status"] == "failed"
    assert any(
        "required column(s): ['seller_id']" in error for error in report["errors"]
    )


# ------------------------------------------------------------
# PRE-PUBLISH VALIDATION GATE
# ------------------------------------------------------------


def test_promote_semantic_version_success(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        valid_seller_dim.to_parquet(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

    report = promote_semantic_version(run_context)

    assert report["status"] == "success"


def test_promote_semantic_version_fails_on_existing_version_directory(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        valid_seller_dim.to_parquet(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

    # Initial run that created the directory
    _ = promote_semantic_version(run_context)

    # Fails due to existing version directory on same run_id
    report = promote_semantic_version(run_context)

    assert report["status"] == "failed"
    assert "Version directory already exists" in report["errors"]


def test_promote_semantic_version_fails_on_making_directory(tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        published_version_path = run_context.version_path / module
        published_version_path.mkdir(parents=True)

    report = promote_semantic_version(run_context)

    assert report["status"] == "failed"
    assert any(
        "File exists" in error or "exists" in error for error in report["errors"]
    )


def test_promote_semantic_version_fails_on_copying_semantic(
    tmp_path,
    monkeypatch,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )
        valid_seller_dim.to_parquet(
            semantic_path / "seller_dim_2023_01.parquet",
            index=False,
        )

        # force shutil.copytree to raise
        def mock_copytree(*args, **kwargs):
            raise RuntimeError("copy failure")

        monkeypatch.setattr(shutil, "copytree", mock_copytree)

    report = promote_semantic_version(run_context)

    assert report["status"] == "failed"
    assert any("copy failure" in error for error in report["errors"])


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

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    df_map = {
        "seller_weekly_fact": valid_seller_fact,
        "seller_dim": valid_seller_dim,
        "customer_weekly_fact": valid_customer_fact,
        "customer_dim": valid_customer_dim,
        "product_weekly_fact": valid_product_fact,
        "product_dim": valid_product_dim,
    }

    for module_name, module in SEMANTIC_MODULES.items():
        module_path = run_context.semantic_path / module_name
        module_path.mkdir(parents=True, exist_ok=True)

        for table_name in module["tables"]:
            df = df_map[table_name]

            df.to_parquet(
                module_path / f"{table_name}_2023_01.parquet",
                index=False,
            )

    report = execute_publish_lifecycle(run_context)

    assert report["status"] == "success"


def test_execute_publish_lifecycle_fails_on_gate(
    tmp_path,
    valid_seller_fact,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    for module in SEMANTIC_MODULES:
        semantic_path = run_context.semantic_path / module
        semantic_path.mkdir(parents=True, exist_ok=True)

        valid_seller_fact.to_parquet(
            semantic_path / "seller_weekly_fact_2023_01.parquet",
            index=False,
        )

        # Missing valid_seller_dim in semantic/

    report = execute_publish_lifecycle(run_context)

    assert report["status"] == "failed"
    assert report["failed_step"] == "integrity_gate"

    step_errors = report["steps"]["integrity_gate"]["errors"]

    assert any("Semantic file set mismatch" in error for error in step_errors)


# =============================================================================
# UNIT TESTS END
# =============================================================================
