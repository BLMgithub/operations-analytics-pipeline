# =============================================================================
# UUIDs to Integers Mappings - Discovery First Architecture
# =============================================================================

import polars as pl
from pathlib import Path
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.storage_adapter import (
    promote_new_mapping_files,
    check_gcs_path_exists,
)

# Maps which tables an ID appears
ID_ENTITY_MAP = {
    "order_id": ["df_orders", "df_order_items", "df_payments"],
    "customer_id": ["df_orders", "df_customers"],
    "product_id": ["df_order_items", "df_products"],
    "seller_id": ["df_order_items"],
}

# -----------------------------------------------------------------------------
# DISCOVERY HELPERS
# -----------------------------------------------------------------------------


def discover_uuids(raw_path: Path, tables: list[str], col: str) -> pl.Series:
    """
    Scans raw snapshot files to identify the unique set of UUIDs present in the current run.

    Contract:
    - Multi-Format Support: Detects and scans both .parquet and .csv extensions.
    - Defensive Schema: Uses 'infer_schema_length=0' for CSVs to ensure ID columns are always treated as strings.
    - Subtractive Consolidation: Aggregates IDs from all relevant tables and enforces global uniqueness.

    Invariants:
    - Type Safety: Always returns a pl.Series of dtype pl.String.
    - Empty Handling: Returns an empty Series with correct column name/type if no files are found.

    Outputs:
    - Unique pl.Series of string UUIDs.
    """

    all_uuids = []

    for table in tables:
        for ext in ["parquet", "csv"]:
            files = list(raw_path.glob(f"{table}_*.{ext}"))
            if not files:
                continue

            table_glob = str(raw_path / f"{table}_*.{ext}").replace("\\", "/")
            if ext == "parquet":
                lf = pl.scan_parquet(table_glob)
            else:
                lf = pl.scan_csv(table_glob, infer_schema_length=0)

            all_uuids.append(lf.select(col))

    if not all_uuids:
        return pl.Series(col, [], dtype=pl.String)

    return pl.concat(all_uuids).unique().collect().get_column(col)


def lookup_mapping_storage(
    storage_glob: str, col: str, batch_uuids: pl.Series
) -> tuple[pl.DataFrame, int]:
    """
    Surgically retrieves known mappings and the current global sequence head from central storage.

    Contract:
    - Sequence Continuity: Resolves the maximum existing integer ID to ensure gapless sequence generation for new IDs.

    Optimization Logic:
    - Semi-Join Hydration: Filters the historical registry against the current batch via semi-join to minimize memory footprint.
    - Parallel Execution: Utilizes pl.collect_all to resolve both mappings and the sequence head in a single IO pass.
    - Early Projection: Restricts the scan to only the required UUID and Integer ID columns.

    Invariants:
    - Integrity: Enforces uniqueness on historical loads to prevent row duplication from overlapping delta files.
    - Grain: Returns a 1-to-1 mapping DataFrame (UUID to UInt32).

    Outputs:
    - tuple: (known_mappings_df, current_max_id_int)

    Failures:
    - System Error: Crashes if storage is unreachable or if the mapping schema is corrupted.
    """
    int_col = f"{col}_int"

    history_lf = pl.scan_parquet(storage_glob, use_statistics=True).select(
        [col, int_col]
    )

    # Find existing UUIDs in this batch
    batch_lf = pl.DataFrame({col: batch_uuids}).lazy()
    known_mappings_plan = history_lf.join(
        batch_lf,
        on=col,
        how="semi",
    ).unique(subset=[col])

    # Extract max mapped IDs
    max_id_plan = history_lf.select(pl.col(int_col).max())

    known_mappings, max_df = pl.collect_all([known_mappings_plan, max_id_plan])

    max_val = max_df.item()
    current_max = max_val if max_val is not None else 0

    return known_mappings, current_max


def generate_and_persist_delta(
    missing_uuids: pl.Series,
    current_max: int,
    col: str,
    runtime_dir: Path,
    run_id: str,
) -> pl.DataFrame:
    """
    Generates deterministic integer mappings for new UUIDs and persists a run-specific delta artifact.

    Contract:
    - Sequence Generation: Assigns UInt32 IDs starting from current_max + 1.
    - Local Persistence: Writes a Parquet delta file to a run-specific directory before promotion.

    Invariants:
    - Determinism: Sequential IDs are stable within a single run's context.
    - Lineage: Delta filename includes the run_id for traceability.

    Outputs:
    - pl.DataFrame of new mappings.

    Failures:
    - Operational: Fails if local disk is unwritable.
    """

    int_col = f"{col}_int"
    start_val = current_max + 1

    new_mappings = pl.DataFrame({col: missing_uuids}).with_columns(
        pl.int_range(start_val, start_val + missing_uuids.len(), dtype=pl.UInt32).alias(
            int_col
        )
    )

    delta_path = runtime_dir / col / f"map_{run_id}.parquet"
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    new_mappings.write_parquet(delta_path)

    return new_mappings


# -----------------------------------------------------------------------------
# MAIN ORCHESTRATOR (ENTRY POINT)
# -----------------------------------------------------------------------------


def extract_entity_mappings(run_context: RunContext) -> dict[str, pl.LazyFrame]:
    """
    Orchestrates the global ID discovery and mapping resolution for the entire pipeline run.

    Workflow:
    1. Discover: Scans raw sources to identify all entity UUIDs (Orders, Customers, etc.) requiring mapping.
    2. Hydrate: Loads historical mappings from central storage for only the discovered UUIDs.
    3. Resolve: Determines which UUIDs are "New" and dispatches them for ID generation.
    4. Promote: Synchronizes all locally generated mapping deltas back to central cloud storage.

    Operational Guarantees:
    - Atomicity: Mappings are resolved once per run to prevent join collisions in the Contract stage.
    - Write Safety: Uses the storage_adapter to promote deltas, ensuring historical files are never overwritten.

    Side Effects:
    - Creates local Parquet deltas in the contracted/id_mapping/ directory.
    - Promotes new mapping files to the central storage bucket.

    Failure Behavior:
    - Fail-Fast: Any error in mapping resolution triggers a RuntimeError to prevent data corruption downstream.
    """
    master_mappings = {}

    raw_path = run_context.raw_snapshot_path
    mapping_dest = run_context.storage_mapping_path
    runtime_dir = run_context.contracted_path / "id_mapping"

    dest_str = str(mapping_dest).replace("\\", "/")
    is_gcs = dest_str.startswith("gs://")

    for entity_col, tables in ID_ENTITY_MAP.items():

        # Extract UUIDs from raw data
        batch_uuids = discover_uuids(raw_path, tables, entity_col)
        if batch_uuids.len() == 0:
            continue

        col_storage_dir = f"{dest_str}/{entity_col}"
        storage_glob = f"{col_storage_dir}/*.parquet"

        # Check mapping in storage
        mapping_exists = (
            check_gcs_path_exists(col_storage_dir)
            if is_gcs
            else Path(col_storage_dir).exists()
        )

        try:
            if mapping_exists:
                known_df, current_max = lookup_mapping_storage(
                    storage_glob, entity_col, batch_uuids
                )
                # Filter new UUIDs from raw data
                missing_uuids = batch_uuids.filter(
                    ~batch_uuids.is_in(known_df.get_column(entity_col))
                )
            else:
                known_df = pl.DataFrame(
                    {entity_col: [], f"{entity_col}_int": []},
                    schema={entity_col: pl.String, f"{entity_col}_int": pl.UInt32},
                )
                missing_uuids = batch_uuids
                current_max = 0

            # Map new UUIDs if found
            if missing_uuids.len() > 0:
                new_df = generate_and_persist_delta(
                    missing_uuids,
                    current_max,
                    entity_col,
                    runtime_dir,
                    run_context.run_id,
                )
                master_mappings[entity_col] = pl.concat([known_df, new_df]).lazy()
            else:
                master_mappings[entity_col] = known_df.lazy()

        except Exception as e:
            raise RuntimeError(f"Master Mapping Failure: {e}") from e

    promote_new_mapping_files(runtime_dir, mapping_dest)

    return master_mappings
