# =============================================================================
# Raw Data Structural Contract Enforcement
# =============================================================================
# - Enforce non-negotiable structural contracts on raw event data
# - Remove records that violate declared schema, key, or temporal invariants
# - Produce a contract-compliant dataset suitable for CI validation and downstream assembly


import pandas as pd
from data_pipeline.shared.loader_exporter import load_logical_table, export_file
from data_pipeline.shared.table_configs import (
    TABLE_CONFIG,
    REQUIRED_TIMESTAMPS,
    TIMESTAMP_FORMATS,
)
from data_pipeline.shared.run_context import RunContext
from typing import List


# ------------------------------------------------------------
# CONTRACT ENFORCEMENT
# ------------------------------------------------------------


def deduplicate_exact_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Exact event deduplication.

    Removes fully identical rows representing the same logical event.
    Returns the cleaned dataframe along with the number of rows removed.
    """

    initial_count = len(df)
    duplicated_mask = df.duplicated()

    if duplicated_mask.any():

        df = df.drop_duplicates()
        removed_count = initial_count - len(df)

    else:
        removed_count = 0

    return df, removed_count


def remove_unparsable_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int, set]:
    """
    Timestamp parse enforcement.

    Removes rows where any required timestamp field cannot be parsed under the declared formats.
    Returns the affected `order_ids` for downstream cascade cleanup.
    """

    initial_count = len(df)
    unparsable_mask = pd.Series(False, index=df.index)

    for col in REQUIRED_TIMESTAMPS:
        ts = pd.to_datetime(
            df[col],
            format=TIMESTAMP_FORMATS[col],
            errors="coerce",
        )

        # accumulate True for every NaT
        unparsable_mask |= ts.isna()

    invalid_order_ids = set()
    if unparsable_mask.any():

        invalid_order_ids = set(df.loc[unparsable_mask, "order_id"])

        df = df[~unparsable_mask]
        remove_count = initial_count - len(df)

    else:
        remove_count = 0

    return df, remove_count, invalid_order_ids


def remove_impossible_timestamps(df: pd.DataFrame) -> tuple[pd.DataFrame, int, set]:
    """
    Temporal invariant enforcement.

    Removes rows that violate required chronological ordering between purchase, approval, and delivery timestamps.

    Returns the affected `order_ids` for downstream cascade cleanup.
    """

    purchase_ts = pd.to_datetime(df["order_purchase_timestamp"])
    approved_ts = pd.to_datetime(df["order_approved_at"])
    delivered_ts = pd.to_datetime(df["order_delivered_timestamp"])

    invalid_mask = (approved_ts < purchase_ts) | (delivered_ts < purchase_ts)
    initial_count = len(df)

    invalid_order_ids = set()
    if invalid_mask.any():

        invalid_order_ids = set(df.loc[invalid_mask, "order_id"])

        df = df[~invalid_mask]
        remove_count = initial_count - len(df)

    else:
        remove_count = 0

    return df, remove_count, invalid_order_ids


def remove_rows_with_null_values(
    df: pd.DataFrame, non_nullable_column: List[str]
) -> tuple[pd.DataFrame, int]:
    """
    Null constraint enforcement.

    Removes rows where any column declared as non-nullable contains null values.
    Returns the cleaned dataframe along with the number of rows removed.
    """

    initial_count = len(df)

    column_nulls = df[non_nullable_column].isna().sum()

    if column_nulls.any():
        df = df.dropna(subset=non_nullable_column)
        removed_count = initial_count - len(df)

    else:
        removed_count = 0

    return df, removed_count


def cascade_drop_by_order_id(
    df: pd.DataFrame, invalid_order_ids: set
) -> tuple[pd.DataFrame, int]:
    """
    Referential cleanup by `order_id`.

    Removes rows whose `order_id` was previously invalidated by
    upstream contract enforcement steps.
    """

    initial_count = len(df)

    df = df[~df["order_id"].isin(invalid_order_ids)]
    removed = initial_count - len(df)

    return df, removed


# ------------------------------------------------------------
# TABLE ROLE CONTRACTS
# ------------------------------------------------------------

ROLE_STEPS = {
    "event_fact": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_unparsable_timestamps,
            "metric": "removed_unparsable_timestamps",
            "args": [],
            "return_invalid_ids": True,
        },
        {
            "contract": remove_impossible_timestamps,
            "metric": "removed_impossible_timestamps",
            "args": [],
            "return_invalid_ids": True,
        },
        {
            "contract": remove_rows_with_null_values,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": False,
        },
    ],
    "transaction_detail": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_rows_with_null_values,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": False,
        },
        {
            "contract": cascade_drop_by_order_id,
            "metric": "removed_cascade_rows",
            "args": ["invalid_order_ids"],
            "return_invalid_ids": False,
        },
    ],
    "entity_reference": [
        {
            "contract": deduplicate_exact_events,
            "metric": "deduplicated_rows",
            "args": [],
            "return_invalid_ids": False,
        },
        {
            "contract": remove_rows_with_null_values,
            "metric": "removed_null_values",
            "args": ["non_nullable"],
            "return_invalid_ids": False,
        },
    ],
}

# ------------------------------------------------------------
# CONTRACT APPLICATION
# ------------------------------------------------------------


def apply_contract(
    run_context: RunContext,
    table_name: str,
    invalid_order_ids: set | None = None,
) -> tuple[dict, set]:
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
        "removed_impossible_timestamps": 0,
        "removed_null_values": 0,
        "status": "running",
        "errors": [],
        "info": [],
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
    non_nullable = config.get("non_nullable_column", [])

    df = load_logical_table(base_path, table_name)

    if df is None:
        report["status"] = "failed"
        report["errors"].append("Failed to load logical table")
        return report, invalid_ids

    report["initial_rows"] = len(df)

    role = config["role"]

    for step in ROLE_STEPS[role]:

        contract = step["contract"]
        args = []

        if "non_nullable" in step["args"]:
            args.append(non_nullable)

        if "invalid_order_ids" in step["args"]:
            args.append(invalid_order_ids)

        try:

            if step["return_invalid_ids"]:
                df, removed, new_invalid = contract(df)
                invalid_ids |= new_invalid

            else:
                df, removed = contract(df, *args)

            report[step["metric"]] += removed

        except Exception as e:
            report["status"] = "failed"
            report["errors"].append(f"{contract.__name__}: Failed at {e}")
            report["final_rows"] = len(df)

            return report, invalid_ids

    report["final_rows"] = len(df)

    output_path = run_context.contracted_path / f"{table_name}_contracted.parquet"

    if not export_file(df, output_path):
        report["status"] = "failed"
        report["errors"].append("Export failed")

    report["status"] = "success"

    return report, invalid_ids


# =============================================================================
# END OF SCRIPT
# =============================================================================
