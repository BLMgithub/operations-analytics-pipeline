# =============================================================================
# Contract Stage Executor
# =============================================================================

import polars as pl
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_single_delta, export_file
from data_pipeline.assembly.assembly_executor import force_gc
from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.contract.registry import ROLE_STEPS
from data_pipeline.contract.id_registrar import ID_ENTITY_MAP


def apply_contract(
    run_context: RunContext,
    table_name: str,
    master_mappings: dict[str, pl.LazyFrame],
    invalid_order_ids: set | None = None,
    valid_order_ids: set | None = None,
) -> tuple[dict, set, set]:
    """
    Orchestrates the Raw-to-Contracted transformation for a specific logical table.

    Workflow:
    1. Resolve: Identifies table metadata (role, schema, keys) from the central registry.
    2. Hydrate: Fetches the raw snapshot from the lake's snapshot zone.
    3. Delegate: Iteratively applies atomic logic rules (Deduplication, Chronology, Null-checks).
    4. Validate: Executes 'enforce_schema' as the terminal structural gate.
    5. Map: Joins against pre-calculated Discovery mappings to enrich UUIDs with UInt32 integer IDs.
    6. Promote: Persists the contract-compliant dataset to the Silver (contracted) zone.

    Operational Guarantees:
    - Subtractive Only: Exclusively filters rows or casts types; never mutates business values.
    - Referential Safety: Propagates invalidated keys across table boundaries to ensure consistent pruning.
    - Structural Finality: Guarantees output parity with the Silver layer specification, including required integer IDs.

    Side Effects:
    - Persists a Parquet artifact to the contracted directory.
    - Updates invalidated 'order_id' sets for downstream cross-table pruning.

    Failure Behavior:
    - Traps logic-step exceptions; logs errors to the report and halts the current table's processing.
    - Crashes if ID mapping joins fail to prevent downstream schema corruption.
    """

    report = {
        "table": table_name,
        "removed_column": 0,
        "initial_rows": 0,
        "final_rows": 0,
        "deduplicated_rows": 0,
        "removed_unparsable_timestamps": 0,
        "removed_cascade_rows": 0,
        "removed_ghost_orphan_rows": 0,
        "removed_impossible_timestamps": 0,
        "removed_null_values": 0,
        "status": "running",
        "errors": [],
        "info": [],
    }

    invalid_ids = set()
    valid_ids = set()

    if invalid_order_ids is None:
        invalid_order_ids = set()
    if valid_order_ids is None:
        valid_order_ids = set()

    if table_name not in TABLE_CONFIG:
        report["status"] = "failed"
        report["errors"].append(f"Unknown table: {table_name}")
        return report, invalid_ids, valid_ids

    base_path = run_context.raw_snapshot_path
    config = TABLE_CONFIG[table_name]
    non_nullable = config.get("non_nullable_column", [])
    required_column = config.get("required_column", [])
    dtypes = config.get("dtypes", {})

    df, filename = load_single_delta(base_path=base_path, table_name=table_name)
    if df is None:
        report["status"] = "failed"
        report["errors"].append("Failed to load logical table")
        return report, invalid_ids, valid_ids

    report["initial_rows"] = len(df)

    role = config["role"]

    for step in ROLE_STEPS[role]:
        contract = step["contract"]

        args = [
            non_nullable if "non_nullable" in step["args"] else None,
            invalid_order_ids if "invalid_order_ids" in step["args"] else None,
            valid_order_ids if "valid_order_ids" in step["args"] else None,
        ]

        # Remove args not needed for the current registry loop
        args = [arg for arg in args if arg is not None]

        if "required_column" in step["args"]:
            args.extend([required_column, dtypes])

        try:
            if step["return_invalid_ids"]:
                df, removed, new_invalid = contract(df, *args)
                invalid_ids |= new_invalid
            else:
                df, removed = contract(df, *args)
            report[step["metric"]] += removed

        except Exception as e:
            report["status"] = "failed"
            report["errors"].append(f"{contract.__name__}: Failed at {e}")
            report["final_rows"] = len(df)

            return report, invalid_ids, valid_ids

    report["final_rows"] = len(df)

    if table_name == "df_orders":
        valid_ids = set(df.get_column("order_id"))

    df_lf = df.lazy()

    try:
        # Attach mapped integer in dataframe
        for entity_col, tables in ID_ENTITY_MAP.items():
            if table_name in tables and entity_col in master_mappings:
                df_lf = df_lf.join(
                    master_mappings[entity_col], on=entity_col, how="left"
                )

    # Force to fail before corrupting downstream
    except Exception as e:
        raise RuntimeError(f"Mapping Uint32 to UUIDs Failed: {e}") from e

    output_path = run_context.contracted_path / f"{filename}.parquet"
    if not export_file(df_lf.collect(), output_path):
        report["status"] = "failed"
        report["errors"].append("Export failed")
        return report, invalid_ids, valid_ids

    if "df" in locals():
        del df
    if "df_lf" in locals():
        del df_lf
    force_gc()

    report["status"] = "success"
    return report, invalid_ids, valid_ids
