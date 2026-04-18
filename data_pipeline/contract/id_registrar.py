# =============================================================================
# UUIDs to Integers Mappings Implementation
# =============================================================================

import polars as pl
from pathlib import Path
import shutil

ID_COLUMNS_TO_MAP = {
    "df_orders": ["order_id", "customer_id"],
    "df_customers": ["customer_id"],
    "df_order_items": ["order_id", "product_id", "seller_id"],
    "df_products": ["product_id"],
    "df_payments": ["order_id"],
}


def map_uuid_to_int(df: pl.DataFrame, mapping_file_path: Path, id_column: str) -> None:
    """
    Enforces idempotency and persistence of UUID-to-Integer mappings for a specific column.

    Contract (Executor):
    - Discovery: Extracts unique UUIDs from the target column to minimize memory footprint.
    - Differential Update: Scans existing registry (if present) to identify UUIDs via set exclusion.
    - Registry Extension: Generates sequential UInt32 identifiers starting from max(existing_id) + 1.
    - Atomic Write: Sinks updated registry to a temporary Parquet file before performing an atomic replace to ensure data integrity.

    Invariants:
    - Guarantees 1:1 mapping between UUID strings and UInt32 integers.
    - Ensures newly assigned IDs are strictly greater than any existing ID in the registry.
    - Never modifies the input DataFrame in-place or returns it.
    """

    int_col_name = f"{id_column}_int"

    delta_ids = df.select(id_column).unique().get_column(id_column)

    if mapping_file_path.exists():
        mapping_lf = pl.scan_parquet(mapping_file_path)

        max_id = mapping_lf.select(pl.col(int_col_name).max()).collect().item()
        max_id = max_id if max_id is not None else 0

        existing_ids = (
            mapping_lf.select(id_column)
            .filter(pl.col(id_column).is_in(delta_ids.to_list()))
            .collect()
            .get_column(id_column)
        )

        # Compare the delta ids to existing record while streaming
        new_uuids = delta_ids.filter(~delta_ids.is_in(existing_ids.to_list()))
        df_new_uuids = pl.DataFrame({id_column: new_uuids})

    else:
        mapping_lf = pl.LazyFrame(
            schema={id_column: pl.String, int_col_name: pl.UInt32}
        )
        max_id = 0
        df_new_uuids = pl.DataFrame({id_column: delta_ids})

    if df_new_uuids.height > 0:
        start_val = max_id + 1

        # Attach new mapped Uint32 ID
        new_mapping_df = df_new_uuids.with_columns(
            pl.int_range(
                start_val, df_new_uuids.height + start_val, dtype=pl.UInt32
            ).alias(int_col_name)
        )

        # Temporarily hold while syncing updates
        temp_map_path = mapping_file_path.with_suffix(".tmp.parquet")

        if mapping_file_path.exists():
            updated_registy_lf = pl.concat([mapping_lf, new_mapping_df.lazy()])
            updated_registy_lf.sink_parquet(temp_map_path)

        else:
            new_mapping_df.write_parquet(temp_map_path)

        temp_map_path.replace(mapping_file_path)


def id_mapping(
    df: pl.DataFrame,
    table_name: str,
    mapping_dict: dict,
    runtime_dir: Path,
    destination: Path,
) -> pl.LazyFrame:
    """
    Orchestrates the two-phase transformation of UUID strings to persistent Integer keys.

    Execution Workflow:
    1. Update (Eager): Iterates through all specified ID columns, updating their respective physical registries on disk.
    2. Promote: Synchronizes newly created mapping files from the runtime environment to central storage.
    3. Transform (Lazy): Constructs a chain of left-joins against the persistent registries to attach integer keys.

    Contract:
    - Integrity: Ensures every unique UUID in the input DataFrame has a corresponding entry in the mapping registry before the join.
    - Efficiency: Utilizes LazyFrame chaining and Parquet scanning to defer data loading until terminal execution.
    - Grain: Maintains the original grain of the input DataFrame; appends {column}_int columns.

    Outputs:
    - A pl.LazyFrame ready for streaming execution, containing original data enriched with integer surrogates.
    """

    cols_to_map = mapping_dict.get(table_name, [])

    if not cols_to_map:
        return df.lazy()

    for id_column in cols_to_map:
        mapping_filename = f"{id_column}_mapping.parquet"
        storage_path = destination / mapping_filename
        temp_path = runtime_dir / mapping_filename

        # Check if mapping exists in storage else create
        target_path = storage_path if storage_path.exists() else temp_path

        if not target_path.parent.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)

        map_uuid_to_int(df, target_path, id_column)

    # Promote new mapping files from runtime directory to central storage
    if runtime_dir.exists():
        destination.mkdir(parents=True, exist_ok=True)
        for file in runtime_dir.glob("*_mapping.parquet"):
            shutil.copy2(file, destination)

    lf_mapped = df.lazy()

    for id_column in cols_to_map:
        mapping_filename = f"{id_column}_mapping.parquet"
        storage_path = destination / mapping_filename

        # Enrich DataFrame with integer surrogates from the registry
        registry_lf = pl.scan_parquet(storage_path)
        lf_mapped = lf_mapped.join(registry_lf, on=id_column, how="left")

    return lf_mapped
