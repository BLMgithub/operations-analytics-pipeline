# =============================================================================
# Contract Stage logic
# =============================================================================

import polars as pl
from typing import List
from data_pipeline.shared.table_configs import REQUIRED_TIMESTAMPS, TIMESTAMP_FORMATS

# ------------------------------------------------------------
# CONTRACT LOGICS
# ------------------------------------------------------------


def deduplicate_exact_events(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
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
    - [Structural] Crashes if input is not a polars DataFrame.
    """

    initial_count = df.height
    duplicated_mask = df.is_duplicated()
    removed_count = 0

    if duplicated_mask.any():

        df = df.unique()
        removed_count = initial_count - df.height

    return df, removed_count


def remove_unparsable_timestamps(df: pl.DataFrame) -> tuple[pl.DataFrame, int, set]:
    """
    Enforces temporal completeness for system-critical fields.

    Contract:
    - Data Presence: Evaluates all columns defined in REQUIRED_TIMESTAMPS for Null/NaT values.
    - Subtractive Filtering: Drops any row containing unmapped temporal data.

    Invariants:
    - Post-Normalization: Operates on the guarantee that the I/O layer has already standardized resolution to microseconds.
    - Referential Integrity: Emits 'order_id' of failing rows to enable cascade pruning downstream.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if REQUIRED_TIMESTAMPS columns are missing from the DataFrame.
    """

    initial_count = df.height
    invalid_order_ids = set()
    remove_count = 0

    exprs = []
    for col in REQUIRED_TIMESTAMPS:
        if col in df.columns:

            if df.schema[col] == pl.String:
                fmt = TIMESTAMP_FORMATS.get(col)
                exprs.append(
                    pl.col(col).str.to_datetime(format=fmt, strict=False).is_null()
                )
            else:
                exprs.append(pl.col(col).is_null())

    unparsable_mask = df.select(pl.any_horizontal(exprs)).to_series()

    if unparsable_mask.any():

        invalid_order_ids = set(df.filter(unparsable_mask).get_column("order_id"))
        df = df.filter(~unparsable_mask)
        remove_count = initial_count - df.height

    return df, remove_count, invalid_order_ids


def remove_impossible_timestamps(df: pl.DataFrame) -> tuple[pl.DataFrame, int, set]:
    """
    Enforces logical chronology for the order lifecycle using Polars expressions.

    Contract:
    - Chronological Gate: Order Approval Date >= Order Purchase Date AND Order Delivery Date >= Order Purchase Date.
    - Subtractive Filtering: Drops rows where the temporal sequence violates physical reality.

    Invariants:
    - Temporal Alignment: Ensures all orders have a positive or zero lead time.
    - Clean Code: Leverages direct Polars comparison logic without manual type-checking overhead.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if lifecycle timestamp columns are missing.
    """

    initial_count = df.height
    invalid_order_ids = set()
    remove_count = 0

    invalid_mask = df.select(
        violation=(
            (pl.col("order_approved_at") < pl.col("order_purchase_timestamp"))
            | (pl.col("order_delivered_timestamp") < pl.col("order_purchase_timestamp"))
        ).fill_null(False)
    ).get_column("violation")

    if invalid_mask.any():
        invalid_order_ids = set(df.filter(invalid_mask).get_column("order_id"))

        df = df.filter(~invalid_mask)
        remove_count = initial_count - df.height

    return df, remove_count, invalid_order_ids


def remove_rows_with_null_constraint(
    df: pl.DataFrame, non_nullable_column: List[str]
) -> tuple[pl.DataFrame, int, set]:
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

    initial_count = df.height
    invalid_ids = set()
    removed_count = 0

    column_nulls = df.select(
        pl.any_horizontal([pl.col(col).is_null() for col in non_nullable_column])
    ).to_series()

    if column_nulls.any():
        if "order_id" in df.columns:
            invalid_ids = set(df.filter(column_nulls).get_column("order_id"))

        df = df.filter(~column_nulls)
        removed_count = initial_count - df.height

    return df, removed_count, invalid_ids


def cascade_drop_by_order_id(
    df: pl.DataFrame, invalid_order_ids: set
) -> tuple[pl.DataFrame, int]:
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

    initial_count = df.height
    removed_count = 0

    df = df.filter(~pl.col("order_id").is_in(invalid_order_ids))
    removed_count = initial_count - df.height

    return df, removed_count


def enforce_parent_reference(
    df: pl.DataFrame, valid_order_ids: set
) -> tuple[pl.DataFrame, int]:
    """
    Enforces referential integrity based on a whitelist of validated keys.

    Contract:
    - Whitelist Filtering: Drops any row whose 'order_id' is NOT present in 'valid_order_ids'.
    - Purpose: Final referential gate to ensure total alignment with the 'orders' grain.

    Invariants:
    - Data Reliability: Guarantees that every child record has a corresponding valid parent.

    Outputs:
    - Tuple: (Filtered DataFrame, Count of dropped rows, Set of invalid order_ids).

    Failures:
    - [Structural] Crashes if 'order_id' column is missing.
    """

    initial_count = df.height
    removed_count = 0

    if not valid_order_ids:
        return df, removed_count

    df = df.filter(pl.col("order_id").is_in(valid_order_ids))
    removed_count = initial_count - df.height

    return df, removed_count


def enforce_schema(
    df: pl.DataFrame, required_column: List[str], dtypes: dict
) -> tuple[pl.DataFrame, int]:
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

    initial_col_count = df.width

    valid_cols = [col for col in required_column if col in df.columns]

    exprs = []
    for col in valid_cols:
        target_dtype = dtypes.get(col)

        if target_dtype == pl.Datetime:

            if df.schema[col] == pl.String:
                fmt = TIMESTAMP_FORMATS.get(col)
                exprs.append(pl.col(col).str.to_datetime(format=fmt, strict=False))
            else:
                exprs.append(pl.col(col))

        elif target_dtype:
            exprs.append(pl.col(col).cast(target_dtype))
        else:
            exprs.append(pl.col(col))

    df = df.select(exprs)
    removed_count = initial_col_count - df.width

    return df, removed_count
