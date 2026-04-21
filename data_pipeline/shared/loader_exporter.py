# =============================================================================
# RAW DATA LOADER AND EXPORTER
# =============================================================================

from pathlib import Path
import polars as pl
from typing import Optional, Callable, Tuple, Any
from google.cloud import bigquery


def normalize_datetimes(lf: pl.LazyFrame | pl.DataFrame) -> Any:
    """
    Standardizes all Datetime columns to a unified resolution (microseconds).

    Contract:
    - Discovery: Scans the schema for all pl.Datetime fields (accepts both LazyFrame and DataFrame).
    - Transformation: Forcefully casts identified columns to 'us' (microseconds) resolution.

    Invariants:
    - Zero-Failure: Returns the input 'lf' unchanged if no Datetime columns are found.
    - Environment Neutrality: Prevents 'Datetime(ns) != Datetime(us)' resolution mismatches
      between local development and cloud production environments.

    Outputs:
    - LazyFrame or DataFrame (matching input type) with resolution-standardized temporal fields.
    """

    schema = lf.collect_schema() if isinstance(lf, pl.LazyFrame) else lf.schema

    datetime_cols = [
        col for col, dtype in schema.items() if isinstance(dtype, pl.Datetime)
    ]
    if not datetime_cols:
        return lf

    return lf.with_columns(
        [pl.col(col).dt.cast_time_unit("us") for col in datetime_cols]
    )


def scan_gcs_uris_from_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> pl.LazyFrame:
    """
    Streams data natively into Polars using BigQuery External Table metadata as a read bridge.

    Contract:
    - Discovery: Uses the BigQuery API to fetch the authoritative 'source_uris' for the External Table.
    - Optimization: Bypasses BigQuery compute and memory-bound Arrow downloads entirely.
    - Zero-Disk Native Streaming: Passes the extracted GCS URIs directly to Polars' Rust-based
      object-store engine for high-performance, concurrent, lazy evaluation from Cloud Storage.

    Invariants:
    - Lazy Evaluation: Returns a pure pl.LazyFrame without executing any I/O blocking reads.
    - Source Consistency: Relies on BigQuery as the source-of-truth for file locations.

    Outputs:
    - A pl.LazyFrame ready for downstream streaming processing.
    """

    if project_id == "PROJECT_ID_NOT_DETECTED":
        raise ValueError(
            "Project ID is set to 'PROJECT_ID_NOT_DETECTED'. Pipeline environment variables are likely missing."
        )

    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        file_query = f"SELECT DISTINCT _FILE_NAME FROM `{table_ref}`"
        query_result = client.query(file_query).result()
        uris = [row[0] for row in query_result]

        if not uris:
            raise ValueError(f"No source URIs found in external table {table_ref}.")

        lfs = [normalize_datetimes(pl.scan_parquet(uri)) for uri in uris]
        lf = pl.concat(lfs, how="vertical_relaxed")

    except Exception as e:
        if log_info:
            log_info(f"Failed to initialize stream for {dataset_id}.{table_id}: {e}")
        raise

    if log_info:
        log_info(
            f"Connected to GCS Stream via BigQuery: {dataset_id}.{table_id} ({len(uris)} URIs)"
        )

    return lf


FILE_LOADERS = {
    ".csv": lambda path: pl.read_csv(path),
    ".parquet": lambda path: pl.read_parquet(path),
}


def load_single_delta(
    base_path: Path | str,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> Tuple[Any, str]:
    """
    Loads the chronologically most recent delta for a logical table.

    Contract:
    - Discovery: Scans 'base_path' for files matching the 'table_name' prefix.
    - Selection: Identifies the target file via alphanumeric sorting of the date suffix (YYYY_MM_DD).
    - Normalization: Automatically applies 'normalize_datetimes' to enforce microsecond resolution.

    Invariants:
    - Recency: Only the latest snapshot is returned; historical deltas are ignored.
    - Format Support: Handles .csv and .parquet (prioritizing Parquet).
    - Source Integrity: Operates on a lazy scan to minimize memory footprint during initial load.

    Outputs:
    - Tuple containing (pl.DataFrame, str: file_name).

    Failures:
    - [Operational] Raises FileNotFoundError if no matching artifacts are found.
    """

    base_path = Path(base_path)

    # Find files matching the table prefix
    files = [
        file
        for file in base_path.iterdir()
        if file.is_file()
        and (file.stem == table_name or file.name.startswith(f"{table_name}_"))
        and file.suffix.lower() in FILE_LOADERS
    ]

    if not files:
        raise FileNotFoundError(f" No file found for {table_name} in {base_path}")

    # Read only recent date suffix
    files = sorted(files)
    target_file = files[-1]

    file_name = target_file.stem
    loader = FILE_LOADERS[target_file.suffix.lower()]

    df = loader(target_file)
    df = normalize_datetimes(df.lazy()).collect()

    if log_info:
        log_info(f"Loaded: {target_file.name} ({len(df)} rows)")

    return df, file_name


def load_historical_data(
    base_path: Path | str,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> pl.LazyFrame:
    """
    Aggregates matching historical artifacts into a single cumulative LazyFrame for the Assembly stage.

    Contract:
    - Discovery: Performs a multi-file glob of all Parquet artifacts matching 'table_name'.
    - Normalize-at-Source: Scans and normalizes resolution (Datetime[us]) for every file individually before concatenation.
    - Safety: Prevents 'Datetime(ns) != Datetime(us)' resolution mismatches that occur when mixing local and cloud Parquet files.

    Invariants:
    - Zero-Loss: Concatenates all identified files into a single unified stream.
    - Lazy Execution: Returns a planned LazyFrame without triggering disk I/O.

    Outputs:
    - Returns a pl.LazyFrame ready for downstream joins and aggregations.

    Failures:
    - [Operational] Raises FileNotFoundError if no Parquet files match the table name in base_path.
    """
    base_path = Path(base_path)

    all_files = list(base_path.glob(f"{table_name}*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No Parquet files found for {table_name}")

    lfs = [normalize_datetimes(pl.scan_parquet(f)) for f in all_files]
    lf_unified = pl.concat(lfs, how="vertical_relaxed")

    if log_info:
        log_info(
            f"Hybrid Scan: {table_name} ({len(all_files)} total files queued for lazy evaluation)"
        )

    return lf_unified


def load_assembled_data(
    base_path: Path,
    table_name: str,
    log_info: Optional[Callable[[str], None]] = None,
) -> pl.LazyFrame:
    """
    Optimized loader for high-volume assembled datasets targeting the Semantic stage.

    Contract:
    - Discovery (Rust): Passes the glob pattern directly to Polars for high-performance file discovery in Rust.
    - Efficiency: Minimizes Python-side overhead by avoiding explicit file listing.
    - Normalization: Applies resolution standardization to the unified scan result.

    Invariants:
    - Consistency: Assumes uniform resolution across assembled files (standardized by the Assembly stage).
    - Validation: Performs a quick existence check before initiating the lazy scan.

    Outputs:
    - A pl.LazyFrame optimized for streaming through semantic model construction.

    Failures:
    - [Operational] Raises FileNotFoundError if no Parquet files matching the pattern are found.
    """

    pattern = str(base_path / f"{table_name}*.parquet")

    if not any(base_path.glob(f"{table_name}*.parquet")):
        raise FileNotFoundError(f"No Parquet files found for {table_name}")

    lf = normalize_datetimes(
        pl.scan_parquet(
            pattern,
            cast_options=pl.ScanCastOptions(datetime_cast="nanosecond-downcast"),
        )
    )

    if log_info:
        log_info(f"Scanned: {table_name} for lazy evaluation")

    return lf


def export_file(
    df: Any,
    output_path: Path,
    log_info: Optional[Callable[[str], None]] = None,
    log_error: Optional[Callable[[str], None]] = None,
    index: bool = False,
) -> bool:
    """
    Persists DataFrames or LazyFrames to disk using standardized formats.

    Contract:
    - Hydrate: Automatically ensures parent directories for 'output_path' exist.
    - Persist: Enforces Parquet with compression as the internal standard.

    Optimization Logic:
    - Streaming Sink: If 'df' is a LazyFrame, uses 'sink_parquet()' to execute
      non-blocking writes directly from the query plan to disk.

    Invariants:
    - Compression: Utilizes 'brotli' for DataFrames and 'snappy' for LazyFrame streaming sinks.

    Outputs:
    - Boolean: True if write succeeded, False on I/O exception.

    Failures:
    - [Operational] Returns False and logs to 'log_error' if disk I/O fails or permissions are denied.
    """

    output_path = Path(output_path)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0

        if isinstance(df, pl.DataFrame):
            df = normalize_datetimes(df)
            df.write_parquet(output_path, compression="brotli")
            row_count = len(df)

        elif isinstance(df, pl.LazyFrame):
            df = normalize_datetimes(df)

            try:
                pa_schema = df.limit(0).collect().to_arrow().schema
                df.sink_parquet(
                    output_path, compression="snappy", arrow_schema=pa_schema
                )

            except Exception as e:
                print(
                    f"[WARNING] Arrow schema override failed, falling back to native sink:{e}"
                )

                df.sink_parquet(output_path, compression="snappy")

            row_count = "streaming"

        else:
            raise TypeError(f"Unsupported DataFrame type provided: {type(df)}")

        if log_info:
            log_info(f"Exported file: {output_path.name} ({row_count} rows)")

        return True

    except Exception as e:
        if log_error:
            log_error(f"Failed to export file {output_path}: {e}")

        return False
