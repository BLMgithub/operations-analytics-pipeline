# =============================================================================
# UNIT TESTS FOR raw_loader_exporter.py
# =============================================================================

import pandas as pd
import pytest

from data_pipeline.shared.raw_loader_exporter import (
    FILE_LOADERS,
    load_logical_table,
    export_file,
)


# ------------------------------------------------------------
# FIXTURES (SHARED TEST DATA)
# ------------------------------------------------------------


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


# ------------------------------------------------------------
# FILE LOADER
# ------------------------------------------------------------


def test_load_logical_table_success(tmp_path, valid_customers_df):

    log = {"info": [], "error": []}

    # logs info for successful file(s) reading
    def info(msg):
        log["info"].append(msg)

    def error(msg):
        log["error"].append(msg)

    valid_customers_df.to_csv(tmp_path / "df_customers_2026_01.csv", index=False)

    df = load_logical_table(tmp_path, "df_customers", log_info=info, log_error=error)

    assert df is not None
    assert len(df) == len(valid_customers_df)
    assert len(log["info"]) == 1
    assert len(log["error"]) == 0


def test_load_logical_table_concat(tmp_path, valid_customers_df):

    valid_customers_df.to_csv(tmp_path / "df_customers_2026_01.csv", index=False)
    valid_customers_df.to_csv(tmp_path / "df_customers_2026_02.csv", index=False)

    df_concat = load_logical_table(tmp_path, "df_customers")

    assert df_concat is not None
    assert len(df_concat) == len(valid_customers_df) * 2


def test_load_logical_table_no_files_found(tmp_path):

    log = {"info": [], "error": []}

    # logs error for no file(s) found
    def error(msg):
        log["error"].append(msg)

    def info(msg):
        log["info"].append(msg)

    df = load_logical_table(tmp_path, "df_customers", log_info=info, log_error=error)

    assert df is None
    assert len(log["info"]) == 0
    assert len(log["error"]) == 1


def test_load_logical_table_fails_on_mixed_file_format(tmp_path, valid_customers_df):

    valid_customers_df.to_csv(tmp_path / "df_customers_2026_01.csv", index=False)
    valid_customers_df.to_parquet(
        tmp_path / "df_customers_2026_02.parquet", index=False
    )

    with pytest.raises(RuntimeError):
        load_logical_table(tmp_path, "df_customers")


def test_load_logical_table_all_matching_files_fails_to_load(tmp_path, monkeypatch):

    def fake_loader(path):
        raise ValueError("failed")

    monkeypatch.setitem(FILE_LOADERS, ".csv", fake_loader)

    (tmp_path / "df_customers_2026_01.csv").write_text("anything")

    df = load_logical_table(tmp_path, "df_customers")

    assert df is None


# ------------------------------------------------------------
# FILE EXPORTER
# ------------------------------------------------------------


def test_export_file_csv_success(tmp_path, valid_customers_df):

    log = {"info": [], "error": []}

    # logs info for successful file(s) export
    def info(msg):
        log["info"].append(msg)

    def error(msg):
        log["error"].append(msg)

    output_path = tmp_path / "out" / "df_customers.csv"

    ok = export_file(valid_customers_df, output_path, log_info=info, log_error=error)

    assert ok is True
    assert output_path.exists()
    assert len(log["info"]) == 1
    assert len(log["error"]) == 0


def test_export_file_parquet_success(tmp_path, valid_customers_df):

    log = {"info": [], "error": []}

    # logs info for successful file(s) export
    def info(msg):
        log["info"].append(msg)

    def error(msg):
        log["error"].append(msg)

    output_path = tmp_path / "out" / "df_customers.csv"

    ok = export_file(valid_customers_df, output_path, log_info=info, log_error=error)

    assert ok is True
    assert output_path.exists()
    assert len(log["info"]) == 1
    assert len(log["error"]) == 0


def test_export_file_fails_on_unsupported_extension(tmp_path, valid_customers_df):

    log = {"info": [], "error": []}

    # logs error for unsupported extension
    def info(msg):
        log["info"].append(msg)

    def error(msg):
        log["error"].append(msg)

    output_path = tmp_path / "df_customers.txt"

    ok = export_file(valid_customers_df, output_path, log_info=info, log_error=error)

    assert ok is False
    assert output_path.exists() is False
    assert len(log["info"]) == 0
    assert len(log["error"]) == 1


def test_export_file_creates_parent_directory(tmp_path, valid_customers_df):

    nested_path = tmp_path / "nested" / "folder" / "file.csv"

    ok = export_file(valid_customers_df, nested_path)

    assert ok is True
    assert nested_path.exists()


# =============================================================================
# UNIT TESTS END
# =============================================================================
