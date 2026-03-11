# =============================================================================
# RAW DATA LOADER AND EXPORTER
# =============================================================================

from pathlib import Path
import pandas as pd
from typing import Optional, Callable


def load_csv_file(path: Path) -> pd.DataFrame:

    return pd.read_csv(path)


def load_parquet_file(
    parquet_path: Path,
) -> pd.DataFrame:

    return pd.read_parquet(parquet_path, engine="pyarrow")


FILE_LOADERS = {
    ".csv": load_csv_file,
    ".parquet": load_parquet_file,
}


def load_logical_table(
    base_path: Path,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
    log_error: Optional[Callable[[str], None]] = None,
) -> Optional[pd.DataFrame]:
    """
    Load and concatenate all CSV/Parquet files belonging to a logical table.

    Files are identified by filename prefix: <table_name>*.csv or <table_name>*.parquet
    """

    base_path = Path(base_path)

    # List valid files and check format with FILE_LOADERS
    files = [
        f
        for f in base_path.iterdir()
        if f.is_file()
        and f.name.startswith(f"{table_name}_")
        and f.suffix.lower() in FILE_LOADERS
    ]

    if not files:
        if log_error:
            log_error(f"{table_name}: no files found in {base_path}")

        return None

    # Prevent mixed file formats
    extensions = {f.suffix.lower() for f in files}
    if len(extensions) > 1:
        raise RuntimeError(f"Mixed file formats detected for {table_name}")

    dfs = []
    files = sorted(files)

    # Route each file using it's format to its registered loader
    for file_path in files:
        loader = FILE_LOADERS[file_path.suffix.lower()]

        try:
            df = loader(file_path)

            if log_info:
                log_info(f"Loaded: {file_path.name} ({len(df)} rows)")

            dfs.append(df)

        except Exception as e:
            if log_error:
                log_error(f"Failed loading: {file_path.name}: {e}")

    if not dfs:
        if log_error:
            log_error(f"{table_name}: all matching files failed to load")
        return None

    return pd.concat(dfs, ignore_index=True)


def export_file(
    df: pd.DataFrame,
    output_path: Path,
    log_info: Optional[Callable[[str], None]] = None,
    log_error: Optional[Callable[[str], None]] = None,
    index: bool = False,
) -> bool:
    """
    Export DataFrame based on file extension (.csv or .parquet).

    Returns True if successful, False otherwise.

    """
    output_path = Path(output_path)

    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        ext = output_path.suffix.lower()

        if ext == ".csv":
            df.to_csv(output_path, index=index)

        elif ext == ".parquet":
            df.to_parquet(output_path, index=index, engine="pyarrow")

        else:
            raise ValueError(
                f'Unsupported file extension: "{ext}". ' "Supported: .csv, .parquet"
            )

        if log_info:
            log_info(
                f"Exported {ext} file: " f"{output_path.name} " f"({len(df)} rows)"
            )

        return True

    except Exception as e:
        if log_error:
            log_error(f"Failed to export file {output_path}: {e}")

        return False
