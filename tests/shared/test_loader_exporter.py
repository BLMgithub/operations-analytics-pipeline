# =============================================================================
# UNIT TESTS FOR loader_exporter.py
# =============================================================================

import pandas as pd
import polars as pl
import pytest
from data_pipeline.shared.loader_exporter import (
    load_single_delta,
    load_historical_table,
    export_file,
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------


@pytest.fixture
def sample_pd_df():
    return pd.DataFrame({"a": [1, 2], "b": [3, 4]})


@pytest.fixture
def sample_pl_df():
    return pl.DataFrame({"a": [1, 2], "b": [3, 4]})


# ------------------------------------------------------------
# LOAD SINGLE DELTA
# ------------------------------------------------------------


def test_load_single_delta_success(tmp_path, sample_pd_df):
    # Setup: Create multiple files with different dates
    # load_single_delta currently uses pandas for loading
    sample_pd_df.to_csv(tmp_path / "df_test_2023_01_01.csv", index=False)

    newer_df = pd.DataFrame({"a": [10], "b": [20]})
    newer_df.to_parquet(tmp_path / "df_test_2023_01_02.parquet", index=False)

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    df, file_name = load_single_delta(tmp_path, "df_test", log_info=logger)

    # Should pick the latest one (alphabetically/chronologically sorted)
    assert file_name == "df_test_2023_01_02"
    assert len(df) == 1
    assert df["a"].iloc[0] == 10
    assert any("Loaded: df_test_2023_01_02.parquet" in msg for msg in log_messages)


def test_load_single_delta_no_files(tmp_path):
    with pytest.raises(FileNotFoundError, match="No file found for df_missing"):
        load_single_delta(tmp_path, "df_missing")


# ------------------------------------------------------------
# LOAD HISTORICAL TABLE
# ------------------------------------------------------------


def test_load_historical_table_success(tmp_path):
    # Setup: Create multiple parquet files using Polars for consistency
    df1 = pl.DataFrame({"id": [1], "val": ["a"]})
    df2 = pl.DataFrame({"id": [2], "val": ["b"]})

    df1.write_parquet(tmp_path / "table_2023_01_01.parquet")
    df2.write_parquet(tmp_path / "table_2023_01_02.parquet")

    log_messages = []

    def logger(msg):
        log_messages.append(msg)

    lf_total = load_historical_table(tmp_path, "table", log_info=logger)

    # load_historical_table returns a LazyFrame now
    assert isinstance(lf_total, pl.LazyFrame)
    df_collected = lf_total.collect()
    assert df_collected.height == 2
    assert set(df_collected["id"].to_list()) == {1, 2}
    assert any(
        "Scanned: table (2 files queued for lazy evaluation)" in msg
        for msg in log_messages
    )


def test_load_historical_table_no_files(tmp_path):
    with pytest.raises(
        FileNotFoundError, match="No Parquet files found for table_missing"
    ):
        load_historical_table(tmp_path, "table_missing")


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

    # Verify content using Polars
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

    # Verify content
    read_df = pl.read_parquet(output_path)
    assert read_df.equals(sample_pl_df)


def test_export_file_unsupported_type(tmp_path, sample_pl_df):
    # export_file currently doesn't check extension but rather type of DF.
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
