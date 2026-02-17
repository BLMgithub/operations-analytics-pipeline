# =============================================================================
# Raw Data Structural Contract Enforcement
# =============================================================================
# - Enforce non-negotiable structural contracts on raw event data
# - Remove records that violate declared schema, key, or temporal invariants
# - Produce a contract-compliant dataset suitable for CI validation and downstream assembly


import pandas as pd
from typing import List
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file
from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from pathlib import Path

# ------------------------------------------------------------
# CONFIGURATIONS
# ------------------------------------------------------------

REQUIRED_TIMESTAMPS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_timestamp",
    "order_estimated_delivery_date",
]


# ------------------------------------------------------------
# CONTRACT ENFORCEMENT
# ------------------------------------------------------------


def deduplicate_exact_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove exact duplicate rows representing the same event.
    """

    initial_count = df.shape[0]
    duplicated_mask = df.duplicated()

    if duplicated_mask.any():

        df = df.drop_duplicates()
        removed_count = initial_count - df.shape[0]

    else:
        removed_count = 0

    return df, removed_count


def remove_unparsable_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int, set]:
    """
    Remove rows where required timestamps cannot be parsed.
    """

    initial_count = df.shape[0]
    unparsable_mask = pd.Series(False, index=df.index)

    for col in REQUIRED_TIMESTAMPS:
        ts = pd.to_datetime(df[col], errors="coerce")

        # accumulate True for every NaT
        unparsable_mask |= ts.isna()

    invalid_order_ids = set()
    if unparsable_mask.any():

        invalid_order_ids = set(df.loc[unparsable_mask, "order_id"])

        df = df[~unparsable_mask]
        remove_count = initial_count - df.shape[0]

    else:
        remove_count = 0

    return df, remove_count, invalid_order_ids


def remove_impossible_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int, set]:
    """
    Remove rows violating declared temporal invariants (e.g. delivery_date < order_date)
    """

    purchase_ts = pd.to_datetime(df["order_purchase_timestamp"])
    approved_ts = pd.to_datetime(df["order_approved_at"])
    delivered_ts = pd.to_datetime(df["order_delivered_timestamp"])

    invalid_mask = (approved_ts < purchase_ts) | (delivered_ts < purchase_ts)
    initial_count = df.shape[0]

    invalid_order_ids = set()
    if invalid_mask.any():

        invalid_order_ids = set(df.loc[invalid_mask, "order_id"])

        df = df[~invalid_mask]
        remove_count = initial_count - df.shape[0]

    else:
        remove_count = 0

    return df, remove_count, invalid_order_ids


def cascade_drop_by_order_id(
    df: pd.DataFrame, invalid_order_ids: set
) -> tuple[pd.DataFrame, int]:
    """
    Remove rows that contains invalid parent primary key (drop from previous enforcement)
    """

    initial_count = df.shape[0]

    df = df[~df["order_id"].isin(invalid_order_ids)]
    removed = initial_count - df.shape[0]

    return df, removed


# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------


def apply_contract(
    run_context: RunContext, table_name: str, invalid_order_ids: set | None = None
) -> tuple[dict, set]:

    report = {
        "table": table_name,
        "initial_rows": 0,
        "final_rows": 0,
        "deduplicated_rows": 0,
        "removed_unparsable_timestamps": 0,
        "removed_cascade_rows": 0,
        "removed_impossible_timestamps": 0,
        "status": "success",
        "errors": [],
    }

    invalid_ids = set()

    if invalid_order_ids is None:
        invalid_order_ids = set()

    if table_name not in TABLE_CONFIG:
        report["status"] = "failed"
        report["errors"].append(f"Unknown table: {table_name}")

        return report, invalid_ids

    base_path = run_context.raw_snapshot_path
    config = TABLE_CONFIG[table_name]

    df = load_logical_table(base_path, table_name)

    if df is None:
        report["status"] = "failed"
        report["errors"].append("Failed to load logical table")
        return report, invalid_ids

    report["initial_rows"] = len(df)

    if config["role"] == "event_fact":

        df, removed = deduplicate_exact_events(df)
        report["deduplicated_rows"] += removed

        df, removed, invalid_1 = remove_unparsable_timestamps(df)
        report["removed_unparsable_timestamps"] += removed

        df, removed, invalid_2 = remove_impossible_timestamps(df)
        report["removed_impossible_timestamps"] += removed

        invalid_ids = invalid_1.union(invalid_2)

    elif config["role"] == "transaction_detail":

        df, removed = deduplicate_exact_events(df)
        report["deduplicated_rows"] += removed

        if invalid_order_ids:
            df, removed = cascade_drop_by_order_id(df, invalid_order_ids)
            report["removed_cascade_rows"] += removed

    elif config["role"] == "entity_reference":

        df, removed = deduplicate_exact_events(df)
        report["deduplicated_rows"] += removed

    report["final_rows"] = len(df)

    output_path = run_context.contracted_path / f"{table_name}_contracted.parquet"

    if not export_file(df, output_path):
        report["status"] = "failed"
        report["errors"].append("Export failed")

    return report, invalid_ids


# =============================================================================
# END OF SCRIPT
# =============================================================================
