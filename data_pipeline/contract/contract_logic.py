# =============================================================================
# Contract Stage logic
# =============================================================================


import pandas as pd
from typing import List
from data_pipeline.shared.table_configs import REQUIRED_TIMESTAMPS, TIMESTAMP_FORMATS


def deduplicate_exact_events(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Enforces record-level uniqueness across the entire row schema.

    Contract:
    - Identifies and removes rows where every column value is an exact match.
    - Retains the 'first' encountered instance of the record.

    Invariants:
    - Grain: Preserves the original semantic grain while purging physical duplicates.

    Outputs:
    - Tuple: (Filtered DataFrame, Integer count of dropped rows).

    Failures:
    - [Structural] Crashes if input is not a pandas DataFrame.
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
    Enforces parseability for system-critical temporal fields.

    Contract:
    - Evaluates all columns defined in REQUIRED_TIMESTAMPS.
    - Subtractive Filtering: Drops any row containing at least one NaT/unparsable value in target columns.

    Invariants:
    - Type Safety: Does not cast types permanently; performs internal validation only.
    - Lineage: Emits 'order_id' of failing rows to enable cascade pruning downstream.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if REQUIRED_TIMESTAMPS columns are missing from the DataFrame.
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
    Enforces logical chronology for the order lifecycle.

    Contract:
    - Chronological Gate: Order Approval Date >= Order Purchase Date AND Order Delivery Date >= Order Purchase Date.
    - Subtractive Filtering: Drops rows where the temporal sequence violates physical reality.

    Invariants:
    - Temporal Alignment: Ensures all orders have a positive or zero lead time.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if lifecycle timestamp columns are missing.
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


def remove_rows_with_null_constraint(
    df: pd.DataFrame, non_nullable_column: List[str]
) -> tuple[pd.DataFrame, int, set]:
    """
    Enforces mandatory data presence (NOT NULL) for a dynamic column list.

    Contract:
    - Subset Validation: Evaluates only columns provided in 'non_nullable_column'.
    - Subtractive Filtering: Drops any row where at least one target column contains Null/NaN.

    Invariants:
    - Data Integrity: Guarantees 100% population for critical join keys and metrics.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if 'non_nullable_column' names are not in the DataFrame.
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
    Enforces referential cleanup based on a blacklist of compromised keys.

    Contract:
    - Blacklist Filtering: Drops any row whose 'order_id' exists in 'invalid_order_ids'.
    - Purpose: Prunes child records (items/payments) whose parent orders failed validation.

    Invariants:
    - Referential Integrity: Prevents orphan records from reaching the assembly stage.

    Outputs:
    - Tuple: (Filtered DataFrame, Integer count of dropped rows).

    Failures:
    - [Structural] Crashes if 'order_id' column is missing.
    """

    initial_count = len(df)

    df = df[~df["order_id"].isin(invalid_order_ids)]
    removed = initial_count - len(df)

    return df, removed


def enforce_parent_reference(
    df: pd.DataFrame, valid_order_ids: set
) -> tuple[pd.DataFrame, int]:
    """
    Enforces referential integrity based on a whitelist of validated keys.

    Contract:
    - Whitelist Filtering: Drops any row whose 'order_id' is NOT present in 'valid_order_ids'.
    - Purpose: Final referential gate to ensure total alignment with the 'orders' grain.

    Invariants:
    - Data Reliability: Guarantees that every child record has a corresponding valid parent.

    Outputs:
    - Tuple: (Filtered DataFrame, Integer count of dropped rows).

    Failures:
    - [Structural] Crashes if 'order_id' column is missing.
    """
    initial_count = len(df)

    if not valid_order_ids:
        return df, 0

    df = df[df["order_id"].isin(valid_order_ids)]
    removed = initial_count - len(df)

    return df, removed


def enforce_schema(
    df: pd.DataFrame, required_column: List[str], dtypes: dict
) -> tuple[pd.DataFrame, int]:
    """
    Finalizes the structural contract via schema projection and type casting.

    Contract:
    - Schema Projection: Drops all columns not explicitly defined in 'required_column'.
    - Type Enforcement: Casts remaining columns to the formats defined in 'dtypes'.

    Invariants:
    - Structural Integrity: Output exactly matches the modeling specification.
    - Grain: Preserves the input row count.

    Outputs:
    - Tuple: (Filtered DataFrame, Integer count of columns removed).

    Failures:
    - [Structural] Crashes if required columns are missing or if dtypes are incompatible.
    """

    initial_col_count = len(df.columns)

    df = df[required_column]
    df = df.astype(dtypes)

    removed = initial_col_count - len(df.columns)

    return df, removed
