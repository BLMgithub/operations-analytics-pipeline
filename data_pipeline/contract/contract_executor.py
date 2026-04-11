# =============================================================================
# Contract Stage Executor
# =============================================================================

from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_single_delta, export_file
from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.contract.registry import ROLE_STEPS


def apply_contract(
    run_context: RunContext,
    table_name: str,
    invalid_order_ids: set | None = None,
    valid_order_ids: set | None = None,
) -> tuple[dict, set, set]:
    """
    Main entry point for the Raw-to-Contracted Stage.

    Workflow:
    1. Resolve: Identifies table metadata (role, schema, keys) from the central registry.
    2. Hydrate: Fetches the raw snapshot from the lake's snapshot zone.
    3. Delegate: Iteratively applies atomic logic rules (Deduplication, Chronology, Null-checks).
    4. Validate: Executes 'enforce_schema' as the terminal structural gate.
    5. Promote: Persists the contract-compliant dataset to the Silver (contracted) zone.

    Operational Guarantees:
    - Subtractive Only: Exclusively filters rows or casts types; never mutates business values.
    - Referential Safety: Propagates invalidated keys across table boundaries to ensure consistent pruning.
    - Structural Finality: Guarantees output parity with the ASSEMBLE_SCHEMA specification.

    Side Effects:
    - Persists a Parquet artifact to the contracted directory.
    - Updates newly invalidated 'order_id' sets for downstream cross-table pruning.

    Failure Behavior:
    - Traps logic-step exceptions; logs errors to the report and halts the current table's processing.

    Returns:
        tuple: (Stage Report Dict, Newly Invalidated IDs Set, Validated Order IDs Set)
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
        args = []

        if "non_nullable" in step["args"]:
            args.append(non_nullable)

        if "invalid_order_ids" in step["args"]:
            args.append(invalid_order_ids)

        if "valid_order_ids" in step["args"]:
            args.append(valid_order_ids)

        if "required_column" in step["args"]:
            args.append(required_column)
            args.append(dtypes)

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
        valid_ids = set(df["order_id"])

    output_path = run_context.contracted_path / f"{filename}.parquet"

    if not export_file(df, output_path):
        report["status"] = "failed"
        report["errors"].append("Export failed")

    report["status"] = "success"

    return report, invalid_ids, valid_ids
