# =============================================================================
# Contract Stage logic
# =============================================================================


import pandas as pd
from data_pipeline.shared.table_configs import (
    REQUIRED_TIMESTAMPS,
    TIMESTAMP_FORMATS,
)
from typing import List


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
) -> tuple[pd.DataFrame, int, set]:
    """
    Null constraint enforcement.

    Removes rows where any column declared as non-nullable contains null values.
    Returns the cleaned dataframe along with the number of rows removed.
    """

    initial_count = len(df)
    invalid_ids = set()

    column_nulls = df[non_nullable_column].isna().any(axis=1)

    if column_nulls.any():
        if "order_id" in df.columns:
            invalid_ids = set(df.loc[column_nulls, "order_id"])

        df = df[~column_nulls]
        removed_count = initial_count - len(df)

    else:
        removed_count = 0

    return df, removed_count, invalid_ids


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


def enforce_parent_reference(
    df: pd.DataFrame, valid_order_ids: set
) -> tuple[pd.DataFrame, int]:
    """
    Referential cleanup using an In-Memory Whitelist.

    Drops any child rows referencing an `order_id` that does not exist
    in the finalized parent dataset.
    """
    initial_count = len(df)

    # If the set is empty (e.g., pipeline just started)
    if not valid_order_ids:
        return df, 0

    df = df[df["order_id"].isin(valid_order_ids)]
    removed = initial_count - len(df)

    return df, removed
