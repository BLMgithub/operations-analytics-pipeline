# =============================================================================
# UNIT TESTS FOR raw_loader_exporter.py
# =============================================================================

import pandas as pd
import pytest

from pathlib import Path
from data_pipeline.shared.raw_loader_exporter import (
    FILE_LOADERS,
    load_logical_table,
    export_file,
)


# ------------------------------------------------------------
# FILE LOADER
# ------------------------------------------------------------


# def test_load_logical_table_successfully(tmp_path):


# def test_load_logical_table_missing_file_logs():


# def test_load_logical_table_fails_on_mix_file_format():


# ------------------------------------------------------------
# FILE EXPORTER
# ------------------------------------------------------------


# def test_export_file_successfully():


# def test_export_file_raise_unsupported_file():


# def test_export_file_fails_on_missing_file_path():
