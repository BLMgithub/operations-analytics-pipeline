# =============================================================================
# RAW DATA LOADER AND EXPORTER
# =============================================================================

from pathlib import Path
import polars as pl
import pandas as pd
from typing import Optional, Callable, Tuple, Literal, Any


PANDAS_FILE_LOADERS = {
    ".csv": lambda path: pd.read_csv(path),
    ".parquet": lambda path: pd.read_parquet(path, engine="pyarrow"),
}

POLARS_FILE_LOADERS = {
    ".csv": lambda path: pl.read_csv(path),
    ".parquet": lambda path: pl.read_parquet(path, use_pyarrow=True),
}


def load_single_delta(
    engine: Literal["Pandas", "Polars"],
    base_path: Path | str,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> Tuple[Any, str]:
    """
    Loads the chronologically most recent delta for a logical table.

    Contract:
    - Scans 'base_path' for files matching the 'table_name' prefix.
    - Identifies the target file via alphanumeric sorting of the date suffix (YYYY_MM_DD).

    Invariants:
    - Recency: Only the latest snapshot is returned; historical deltas are ignored.
    - Format Support: Handles .csv and .parquet (prioritizing Parquet).

    Failures:
    - Raises FileNotFoundError if no matching artifacts are found.
    """

    base_path = Path(base_path)

    # Find files matching the table prefix
    files = [
        file
        for file in base_path.iterdir()
        if file.is_file()
        and (file.stem == table_name or file.name.startswith(f"{table_name}_"))
        and file.suffix.lower() in PANDAS_FILE_LOADERS
    ]

    if not files:
        raise FileNotFoundError(f" No file found for {table_name} in {base_path}")

    # Read only recent date suffix
    files = sorted(files)
    target_file = files[-1]

    file_name = target_file.stem

    if engine == "Pandas":
        loader = PANDAS_FILE_LOADERS[target_file.suffix.lower()]
    else:
        loader = POLARS_FILE_LOADERS[target_file.suffix.lower()]

    df = loader(target_file)

    if log_info:
        log_info(f"Loaded: {target_file.name} ({len(df)} rows)")

    return df, file_name


def load_historical_table(
    base_path: Path | str,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> pl.DataFrame:
    """
    Aggregates all matching artifacts into a single cumulative DataFrame.

    Contract:
    - Performs a multi-file read of all artifacts matching the 'table_name'.
    - Concatenates results into a single memory object with index resetting.

    Outputs:
    - Returns a unified DataFrame.
    """
    base_path = Path(base_path)

    # files = [
    #     f
    #     for f in base_path.iterdir()
    #     if f.is_file()
    #     and (f.stem == table_name or f.name.startswith(f"{table_name}_"))
    #     and f.suffix.lower() == ".parquet"
    # ]

    files = list(base_path.glob(f"{table_name}*.parquet"))

    if not files:
        raise FileNotFoundError(f"No Parquet files found for {table_name}")

    # def get_unified_data(file_list: list[Path]) -> pd.DataFrame:
    #     temp_dfs = []
    #     for file_path in sorted(file_list):
    #         df = pd.read_parquet(file_path, engine="pyarrow")
    #         temp_dfs.append(df)

    #     return pd.concat(temp_dfs, ignore_index=True)

    # df_unified = get_unified_data(files)

    # if log_info:
    #     log_info(f"Loaded unified: {table_name} ({len(df_unified)} rows)")

    df_unified = pl.read_parquet(files)

    if log_info:
        log_info(f"Loaded unified: {table_name} ({len(df_unified)} rows)")

    return df_unified


def export_file(
    df: pl.DataFrame | pd.DataFrame,
    output_path: Path,
    log_info: Optional[Callable[[str], None]] = None,
    log_error: Optional[Callable[[str], None]] = None,
    index: bool = False,
) -> bool:
    """
    Persists DataFrames to disk using system-standard technical formats.

    Contract:
    - Automates directory creation for the target 'output_path'.
    - Enforces Parquet with Brotli compression as the internal standard.

    Invariants:
    - Compression: Parquet exports always utilize 'brotli' to optimize storage.

    Returns:
        bool: True if write succeeded, False on I/O exception.
    """

    output_path = Path(output_path)

    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # ext = output_path.suffix.lower()

        # if ext == ".csv":
        #     df.to_csv(output_path, index=index)

        # elif ext == ".parquet":
        #     df.to_parquet(
        #         output_path, index=index, engine="pyarrow", compression="brotli"
        #     )

        # else:
        #     raise ValueError(
        #         f'Unsupported file extension: "{ext}". ' "Supported: .csv, .parquet"
        #     )

        if isinstance(df, pd.DataFrame):
            df.to_parquet(
                output_path, index=index, engine="pyarrow", compression="brotli"
            )

        else:
            df.write_parquet(output_path, compression="brotli")

        if log_info:
            log_info(f"Exported file: " f"{output_path.name} " f"({len(df)} rows)")

        return True

    except Exception as e:
        if log_error:
            log_error(f"Failed to export file {output_path}: {e}")

        return False
