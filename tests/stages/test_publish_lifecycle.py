# =============================================================================
# UNIT TESTS FOR publish_lifecycle.py
# =============================================================================

import pandas as pd
import pytest
import shutil

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.publish_lifecycle import (
    init_report,
    log_info,
    log_error,
    run_integrity_gate,
    promote_semantic_version,
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
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    valid_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    report = run_integrity_gate(run_context)

    assert "success" in report["status"]
    assert "Pre-publishing validation passed" in report["info"]


def test_run_integrity_gate_fails_on_missing_directory(tmp_path):

    run_context = RunContext.create(base_path=tmp_path)

    report = run_integrity_gate(run_context)

    assert "failed" in report["status"]
    assert "Semantic directory is missing" in report["errors"]


def test_run_integrity_gate_fails_on_semantic_file_mismatch(
    tmp_path,
    valid_seller_fact,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    # Missing valid_seller_dim in semantic/
    report = run_integrity_gate(run_context)

    assert "failed" in report["status"]
    assert "Semantic file set mismatch" in report["errors"]


def test_run_integrity_gate_fails_on_loading_parquet_files(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact.to_csv(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    valid_seller_dim.to_csv(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    report = run_integrity_gate(run_context)

    assert "failed" in report["status"]
    assert any("parquet failed to load" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_empty_dataframe(tmp_path):

    empty_seller_fact = pd.DataFrame()
    empty_seller_dim = pd.DataFrame()

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    empty_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    empty_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    report = run_integrity_gate(run_context)

    assert "failed" in report["status"]
    assert any("logical table missing or empty" in error for error in report["errors"])


def test_run_integrity_gate_fails_on_missing_columns(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact = valid_seller_fact.drop(columns="seller_id")

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    valid_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    report = run_integrity_gate(run_context)

    assert "failed" in report["status"]
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

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    valid_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    report = promote_semantic_version(run_context)

    assert "success" in report["status"]
    assert "Semantic artifacts promoted successfully" in report["info"]


def test_promote_semantic_version_fails_on_existing_version_directory(
    tmp_path,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )

    valid_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    # Initial run that created the directory
    _ = promote_semantic_version(run_context)

    # Fails due to existing version directory on same run_id
    report = promote_semantic_version(run_context)

    assert "failed" in report["status"]
    assert "Version directory already exists" in report["errors"]


def test_promote_semantic_version_fails_on_making_directory(tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    # Force mkdir to raise
    run_context.version_path.mkdir(parents=True)

    report = promote_semantic_version(run_context)

    assert report["status"] == "failed"
    assert any("File exists" in e or "exists" in e for e in report["errors"])


def test_promote_semantic_version_fails_on_copying_semantic(
    tmp_path,
    monkeypatch,
    valid_seller_fact,
    valid_seller_dim,
):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")
    run_context.initialize_directories()

    valid_seller_fact.to_parquet(
        run_context.semantic_path / "seller_week_performance_fact_2023_01.parquet",
        index=False,
    )
    valid_seller_dim.to_parquet(
        run_context.semantic_path / "seller_dim_2023_01.parquet",
        index=False,
    )

    # force shutil.copy2 to raise
    def mock_copy2(*args, **kwargs):
        raise RuntimeError("copy failure")

    monkeypatch.setattr(shutil, "copy2", mock_copy2)

    report = promote_semantic_version(run_context)

    assert report["status"] == "failed"
    assert any("copy failure" in e for e in report["errors"])


# =============================================================================
# UNIT TESTS END
# =============================================================================
