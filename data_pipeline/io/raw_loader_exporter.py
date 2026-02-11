# =============================================================================
# RAW DATA LOADER AND EXPORTER
# =============================================================================

import pandas as pd
import glob
from typing import Optional, Callable, Literal
import os

def load_csv_file(csv_path: str, 
                  table_name: str,
                  log_info: Optional[Callable[[str], None]] = None,
                  log_error: Optional[Callable[[str], None]] = None,
                  ) -> Optional[pd.DataFrame]:

    try:
        df = pd.read_csv(csv_path)

        if log_info:
            log_info(
                f'Loaded {table_name} file: {os.path.basename(csv_path)} ({len(df)} rows)'
            )

        return df

    except Exception as e:
        if log_error:
            log_error(
                f'Failed to load {table_name} file {csv_path}: {e}'
            )
        return None


def load_logical_table(partition_path: str,
                       table_name: str,
                       log_info: Optional[Callable[[str], None]] = None,
                       log_error: Optional[Callable[[str], None]] = None,
                       ) -> Optional[pd.DataFrame]:
    """
    Load and concatenate all CSV files belonging to a logical table.

    Files are identified by filename prefix: <table_name>*.csv
    """

    pattern = os.path.join(partition_path, f'{table_name}*.csv')
    csv_files = glob.glob(pattern)

    if not csv_files:
        if log_error:
            log_error(
                f'{table_name}: no files found matching pattern {pattern}'
            )
        return None

    dfs = []
    for csv_path in csv_files:
        df = load_csv_file(
            csv_path,
            table_name,
            log_info=log_info,
            log_error=log_error,
        )
        if df is not None:
            dfs.append(df)

    if not dfs:
        if log_error:
            log_error(
                f'{table_name}: all matching files failed to load'
            )
        return None

    combined_df = pd.concat(dfs, ignore_index=True)

    if log_info:
        log_info(
            f'{table_name}: combined {len(csv_files)} file(s) into '
            f'{len(combined_df)} rows'
        )

    return combined_df


def export_file(df: pd.DataFrame,
                output_path: str,
                log_info: Optional[Callable[[str], None]] = None,
                log_error: Optional[Callable[[str], None]] = None,
                index: bool = False,
                ) -> bool:
    """
    Export DataFrame based on file extension (.csv or .parquet).

    Returns True if successful, False otherwise.
    """

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        _, ext = os.path.splitext(output_path)
        ext = ext.lower()

        if ext == '.csv':
            df.to_csv(output_path, index=index)

        elif ext == '.parquet':
            df.to_parquet(output_path, index=index, engine='pyarrow')

        else:
            raise ValueError(
                f'Unsupported file extension: "{ext}". '
                'Supported: .csv, .parquet'
            )

        if log_info:
            log_info(
                f'Exported {ext} file: '
                f'{os.path.basename(output_path)} '
                f'({len(df)} rows)'
            )

        return True

    except Exception as e:
        if log_error:
            log_error(
                f'Failed to export file {output_path}: {e}'
            )
        return False