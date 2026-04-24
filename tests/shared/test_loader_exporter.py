# =============================================================================
# UNIT TESTS FOR loader_exporter.py
# =============================================================================

import polars as pl
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from data_pipeline.shared.loader_exporter import (
    normalize_datetimes,
    scan_gcs_uris_from_bigquery,
    load_single_delta,
    load_historical_data,
    load_assembled_data,
    export_file,
)

# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------


@pytest.fixture
def sample_pl_df():
    return pl.DataFrame({"a": [1, 2], "b": [3, 4]})


# ------------------------------------------------------------
# NORMALIZE DATETIMES
# ------------------------------------------------------------


def test_normalize_datetimes():
    # With nanosecond
    df = pl.DataFrame({"ts": [datetime(2023, 1, 1)], "val": [1]}).with_columns(
        pl.col("ts").dt.cast_time_unit("ns")
    )

    lf = df.lazy()
    assert lf.collect_schema()["ts"].time_unit == "ns"  # type: ignore

    normalized_lf = normalize_datetimes(lf)
    assert normalized_lf.collect_schema()["ts"].time_unit == "us"  # type: ignore


def test_normalize_datetimes_no_temporal_cols():
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    lf = df.lazy()
    normalized_lf = normalize_datetimes(lf)
    assert normalized_lf is lf


# ------------------------------------------------------------
# BIGQUERY EXTERNAL TABLE SCAN (Mocked IO)
# ------------------------------------------------------------


def test_scan_gcs_uris_from_bigquery_success():
    project_id = "test-project"
    dataset_id = "test_dataset"
    table_id = "test_table"

    # Mock BigQuery Client and Result
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        ["gs://bucket/file1.parquet"],
        ["gs://bucket/file2.parquet"],
    ]
    mock_client.query.return_value = mock_query_job

    with patch("google.cloud.bigquery.Client", return_value=mock_client), patch(
        "polars.scan_parquet"
    ) as mock_scan_parquet:

        # Mock the Polars LazyFrame returned by scan_parquet
        mock_lf = pl.LazyFrame({"a": [1]})
        mock_scan_parquet.return_value = mock_lf

        lf = scan_gcs_uris_from_bigquery(project_id, dataset_id, table_id)

        # Check BigQuery interactions
        mock_client.query.assert_called_once()
        query_call = mock_client.query.call_args[0][0]
        assert "SELECT DISTINCT _FILE_NAME" in query_call
        assert f"`{project_id}.{dataset_id}.{table_id}`" in query_call

        # Check Polars interactions
        assert mock_scan_parquet.call_count == 2
        mock_scan_parquet.assert_any_call("gs://bucket/file1.parquet")
        mock_scan_parquet.assert_any_call("gs://bucket/file2.parquet")
        assert lf is not None


def test_scan_gcs_uris_from_bigquery_empty_results():
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    with (
        patch("google.cloud.bigquery.Client", return_value=mock_client),
        pytest.raises(ValueError, match="No source URIs found"),
    ):
        scan_gcs_uris_from_bigquery("proj", "ds", "tbl")


def test_scan_gcs_uris_from_bigquery_invalid_env():
    with pytest.raises(ValueError, match="Project ID is set to"):
        scan_gcs_uris_from_bigquery("PROJECT_ID_NOT_DETECTED", "ds", "tbl")


# ------------------------------------------------------------
# LOAD SINGLE DELTA
# ------------------------------------------------------------


def test_load_single_delta_success(tmp_path, sample_pl_df):
    sample_pl_df.write_csv(tmp_path / "df_test_2023_01_01.csv")

    newer_df = pl.DataFrame({"a": [10], "b": [20]})
    newer_df.write_parquet(tmp_path / "df_test_2023_01_02.parquet")

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    df, file_name = load_single_delta(tmp_path, "df_test", log_info=logger)

    # Should pick the latest one (chronologically sorted)
    assert file_name == "df_test_2023_01_02"
    assert len(df) == 1
    assert df[0, "a"] == 10
    assert any("Loaded: df_test_2023_01_02.parquet" in msg for msg in log_messages)


def test_load_single_delta_no_files(tmp_path):
    with pytest.raises(FileNotFoundError, match="No file found for df_missing"):
        load_single_delta(tmp_path, "df_missing")


# ------------------------------------------------------------
# LOAD HISTORICAL TABLE
# ------------------------------------------------------------


def test_load_historical_data_success(tmp_path):

    df1 = pl.DataFrame({"id": [1], "val": ["a"]})
    df2 = pl.DataFrame({"id": [2], "val": ["b"]})

    df1.write_parquet(tmp_path / "table_2023_01_01.parquet")
    df2.write_parquet(tmp_path / "table_2023_01_02.parquet")

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    lf_total = load_historical_data(tmp_path, "table", log_info=logger)

    assert isinstance(lf_total, pl.LazyFrame)
    df_collected = lf_total.collect()
    assert df_collected.height == 2
    assert set(df_collected["id"].to_list()) == {1, 2}


def test_load_historical_data_no_files(tmp_path):
    with pytest.raises(
        FileNotFoundError, match="No Parquet files found for table_missing"
    ):
        load_historical_data(base_path=tmp_path, table_name="table_missing")


# ------------------------------------------------------------
# LOAD ASSEMBLED DATA
# ------------------------------------------------------------


def test_load_assembled_data_success(tmp_path):
    table_name = "assembled_table"
    df = pl.DataFrame({"a": [1]})
    df.write_parquet(tmp_path / f"{table_name}_part1.parquet")

    lf = load_assembled_data(tmp_path, table_name)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().height == 1


def test_load_assembled_data_no_files(tmp_path):
    with pytest.raises(
        FileNotFoundError, match="No Parquet files found for missing_assembled"
    ):
        load_assembled_data(tmp_path, "missing_assembled")


# ------------------------------------------------------------
# EXPORT FILE
# ------------------------------------------------------------


def test_export_file_parquet_success(tmp_path, sample_pl_df):
    output_path = tmp_path / "output" / "data.parquet"

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    # Test with Polars DataFrame
    success = export_file(sample_pl_df, output_path, log_info=logger)

    assert success is True
    assert output_path.exists()
    assert any("Exported file: data.parquet (2 rows)" in msg for msg in log_messages)

    read_df = pl.read_parquet(output_path)
    assert read_df.equals(sample_pl_df)


def test_export_file_lazy_success(tmp_path, sample_pl_df):
    output_path = tmp_path / "output" / "lazy_data.parquet"

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    # Test with Polars LazyFrame
    success = export_file(sample_pl_df.lazy(), output_path, log_info=logger)

    assert success is True
    assert output_path.exists()
    assert any(
        "Exported file: lazy_data.parquet (streaming rows)" in msg
        for msg in log_messages
    )

    read_df = pl.read_parquet(output_path)
    assert read_df.equals(sample_pl_df)


def test_export_file_unsupported_type(tmp_path, sample_pl_df):

    error_messages = []

    def error_logger(msg):
        error_messages.append(msg)

    success = export_file(
        "not a dataframe", tmp_path / "fail.parquet", log_error=error_logger
    )

    assert success is False
    assert any("Unsupported DataFrame type" in msg for msg in error_messages)


def test_export_file_handles_io_error(tmp_path, sample_pl_df):
    # Try to write to a path that is actually a directory
    bad_path = tmp_path / "is_a_dir.parquet"
    bad_path.mkdir()

    error_messages = []

    def error_logger(msg):
        error_messages.append(msg)

    success = export_file(sample_pl_df, bad_path, log_error=error_logger)

    assert success is False
    assert any("Failed to export file" in msg for msg in error_messages)
