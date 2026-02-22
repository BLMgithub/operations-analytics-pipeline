# =============================================================================
# Raw Data Structural Contract Enforcement
# =============================================================================
# - Enforce non-negotiable structural contracts on raw event data
# - Remove records that violate declared schema, key, or temporal invariants
# - Produce a contract-compliant dataset suitable for CI validation and downstream assembly


import pandas as pd
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file
from data_pipeline.shared.table_configs import (
    TABLE_CONFIG,
    REQUIRED_TIMESTAMPS,
    TIMESTAMP_FORMATS,
)
from data_pipeline.shared.run_context import RunContext


# ------------------------------------------------------------
# CONTRACT ENFORCEMENT
# ------------------------------------------------------------


def deduplicate_exact_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Exact event deduplication.

    Removes fully identical rows representing the same logical event and <br>
    returns the cleaned dataframe along with the number of rows removed.
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

    Removes rows where any required timestamp field cannot be parsed under
    the declared formats. <br>
    Also returns the affected `order_ids` to support downstream cascade cleanup.
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

    Removes rows that violate required chronological ordering between
    purchase, approval, and delivery timestamps. <br>
    Also returns the affected `order_ids` for downstream cascade cleanup.
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
# CONTRACT APPLICATION
# ------------------------------------------------------------


def apply_contract(
    run_context: RunContext, table_name: str, invalid_order_ids: set | None = None
) -> tuple[dict, set]:
    """
    Applies role-driven deterministic normalization, producing a cleaned
    dataset and a structured execution report.

    Chronological behavior:
    - Initializes contract metrics and error container.
    - Validates table eligibility against TABLE_CONFIG.
    - Loads the logical table from the raw snapshot.
    - Applies role-specific contract steps:
        - **event_fact:**
            - exact deduplication
            - timestamp parse enforcement
            - temporal invariant enforcement (produces invalid `order_ids`)
        - **transaction_detail:**
            - exact deduplication
            - optional cascade removal using upstream invalid `order_ids`
        - **entity_reference:**
            - exact deduplication only.
    - Records row-level impact for each enforcement step.
    - Writes the contracted output to the contract layer.
    - Returns the execution report and any newly invalidated `order_ids`.
    """

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
