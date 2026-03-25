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
    Enforce structural contract on a single logical table.

    Role-driven behavior:
    - event_fact:
        - exact deduplication
        - remove unparsable timestamps
        - remove temporal violations
        - emit invalid order_ids
        - remove null rows
    - transaction_detail:
        - deduplicate
        - cascade drop invalid order_ids
        - remove null rows
    - entity_reference:
        - deduplicate
        - remove null rows

    Guarantees:
    - Deterministic row removal
    - No numeric or domain correction
    - Output written to contracted layer

    Returns:
    - Contract execution report
    - Newly invalidated order_ids (if any)
    """

    report = {
        "table": table_name,
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

    df, filename = load_single_delta(base_path, table_name)

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
