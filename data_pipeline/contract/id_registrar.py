# =============================================================================
# UUIDs to Integers Mappings
# =============================================================================

import polars as pl
from pathlib import Path
from data_pipeline.shared.storage_adapter import (
    promote_new_mapping_files,
    check_gcs_path_exists,
)

ID_COLUMNS_TO_MAP = {
    "df_orders": ["order_id", "customer_id"],
    "df_customers": ["customer_id"],
    "df_order_items": ["order_id", "product_id", "seller_id"],
    "df_products": ["product_id"],
    "df_payments": ["order_id"],
}


def map_uuid_to_int(
    df: pl.DataFrame,
    id_column: str,
    storage_destination: str | Path,
    local_output_path: Path,
) -> None:
    """
    Enforces idempotency and persistence of UUID-to-Integer mappings using a Delta
        pattern.

    Contract:
    - Discovery: Scans ALL existing mapping deltas from storage (glob) to find max_id
        and existing UUIDs.
    - Differential Update: Identifies only the new UUIDs not present in history.
    - Registry Extension: Generates new IDs starting from max(existing) + 1.
    - Delta Write: Saves ONLY the new mappings to a local run-specific file for later
        promotion.
    """

    int_col_name = f"{id_column}_int"
    delta_ids = df.select(id_column).unique().get_column(id_column)

    # Resolve Storage Path (Local or GCS)
    dest_str = str(storage_destination).replace("\\", "/")
    is_gcs = dest_str.startswith("gs://")

    column_storage_dir = f"{dest_str}/{id_column}"
    storage_glob = f"{column_storage_dir}/*.parquet"

    # Check existence and Scan history
    exists = (
        check_gcs_path_exists(column_storage_dir)
        if is_gcs
        else Path(column_storage_dir).exists()
    )

    if exists:
        mapping_lf = pl.scan_parquet(storage_glob)

        # Get max ID across all deltas
        max_id = mapping_lf.select(pl.col(int_col_name).max()).collect().item()
        max_id = max_id if max_id is not None else 0

        existing_ids = (
            mapping_lf.select(id_column)
            .filter(pl.col(id_column).is_in(delta_ids.to_list()))
            .collect()
            .get_column(id_column)
        )
        new_uuids = delta_ids.filter(~delta_ids.is_in(existing_ids.to_list()))
    else:
        max_id = 0
        new_uuids = delta_ids

    # If new UUIDs found, write a new detla file
    if new_uuids.len() > 0:
        start_val = max_id + 1
        new_mapping_df = pl.DataFrame({id_column: new_uuids}).with_columns(
            pl.int_range(start_val, new_uuids.len() + start_val, dtype=pl.UInt32).alias(
                int_col_name
            )
        )

        local_output_path.parent.mkdir(parents=True, exist_ok=True)
        new_mapping_df.write_parquet(local_output_path)


def id_mapping(
    df: pl.DataFrame,
    table_name: str,
    mapping_dict: dict,
    runtime_dir: Path,
    destination: Path | str,
    run_id: str,
) -> pl.LazyFrame:
    """
    Orchestrates the transformation of UUID strings to persistent Integer keys using
        Delta Architecture.

    Execution Workflow:
    1. Update (Delta): Creates new mapping files only for previously unseen UUIDs.
    2. Promote: Uploads the tiny new delta files to the central storage directory.
    3. Transform (Lazy): Joins the input against the full set of deltas (glob scan).
    """

    cols_to_map = mapping_dict.get(table_name, [])
    if not cols_to_map:
        return df.lazy()

    for id_column in cols_to_map:
        local_delta_path = runtime_dir / id_column / f"map_{run_id}.parquet"

        map_uuid_to_int(
            df=df,
            id_column=id_column,
            storage_destination=destination,
            local_output_path=local_delta_path,
        )

    promote_new_mapping_files(runtime_dir=runtime_dir, destination=destination)

    # Apply mapped ids to dataframe
    dest_str = str(destination).replace("\\", "/")
    lf_mapped = df.lazy()

    for id_column in cols_to_map:
        storage_glob = f"{dest_str}/{id_column}/*.parquet"

        registry_lf = pl.scan_parquet(storage_glob)
        lf_mapped = lf_mapped.join(registry_lf, on=id_column, how="left")

    return lf_mapped
