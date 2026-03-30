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

    This component enforces structural data quality gates based on the logical
    role of the table. It acts as a subtractive filter and schema-freezer,
    ensuring only compliant rows and columns reach the Silver (contracted) layer.

    Workflow:
        1. Resolve: Determines table configuration and role (event_fact, entity_reference, etc.).
        2. Load: Fetches the raw snapshot from the lake's snapshot zone.
        3. Sequence: Iteratively applies atomic filtering rules (Deduplication, Null-checks, etc.).
        4. Track: Captures row-level telemetry and identifies compromised 'order_id's.
        5. Propagate: Returns validated/invalidated IDs to maintain referential integrity.
        6. Freeze: Executes 'enforce_schema' as the terminal step to project approved columns.
        7. Export: Persists the contract-compliant dataset to the Silver zone.

    Operational Guarantees:
    - Subtractive Only: Filters rows first; never mutates row values (only column types).
    - Finality: The 'enforce_schema' step guarantees the artifact matches the system registry.
    - Referential Integrity: Tables processed after 'df_orders' use its output for parent-check filtering.

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

    df, filename = load_single_delta(
        engine="Pandas", base_path=base_path, table_name=table_name
    )

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
