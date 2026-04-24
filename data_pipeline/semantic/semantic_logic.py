# =============================================================================
# Semantic Modelinc Stage Logic
# =============================================================================

import polars as pl
from typing import Dict
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_assembled_data

# ------------------------------------------------------------
# SELLER SEMANTIC BUILDER
# ------------------------------------------------------------


def build_seller_semantic(lf: pl.LazyFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Seller-centric analytical layer from assembled integer-mapped events.

    Contract:
    - Subtractive Filtering: Selects strictly required columns for performance.
    - Transformation: Derives week_start_date and boolean status flags.
    - Aggregation: Computes weekly performance metrics (revenue, lead times, delays) per seller using optimized Integer keys.

    Optimization Logic:
    - Integer Key Optimization: Utilizes UInt32/UInt64 grouping keys (seller_id_int) to maintain a constant memory profile during non-blocking aggregation.
    - Metric Downcasting: Enforces Int16 for counts/days and Float32 for revenue to minimize row width during streaming.

    Invariants:
    - Fact Grain: Strictly 1 row per ('seller_id_int', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'seller_id_int'.
    - Temporal: Aligns all metrics to ISO-week start dates (Monday).

    Outputs:
    - Dict containing 'seller_weekly_fact' (LazyFrame) and 'seller_dim' (LazyFrame).

    Failures:
    - [Structural] Crashes if input LazyFrame lacks required columns.
    """

    needed_cols = [
        "seller_id_int",
        "order_year_week",
        "order_date",
        "order_status",
        "order_id_int",
        "order_revenue",
        "lead_time_days",
        "delivery_delay_days",
        "approval_lag_days",
    ]

    lf_filtered = lf.select(needed_cols)

    seller_weekly_fact = (
        lf_filtered.with_columns(
            week_start_date=pl.col("order_date").dt.truncate("1w"),
            is_delivered=pl.col("order_status").eq("delivered"),
            is_cancelled=pl.col("order_status").eq("cancelled"),
        )
        .group_by(["seller_id_int", "order_year_week"])
        .agg(
            week_start_date=pl.col("week_start_date").min(),
            weekly_order_count=pl.col("order_id_int").count().cast(pl.Int16),
            weekly_delivered_orders=pl.col("is_delivered").sum().cast(pl.Int16),
            weekly_cancelled_orders=pl.col("is_cancelled").sum().cast(pl.Int16),
            weekly_revenue=pl.col("order_revenue").sum().cast(pl.Float32),
            weekly_avg_lead_time=pl.col("lead_time_days").mean().cast(pl.Float32),
            weekly_total_lead_time=pl.col("lead_time_days").sum().cast(pl.Int16),
            weekly_avg_delivery_delay=pl.col("delivery_delay_days")
            .mean()
            .cast(pl.Float32),
            weekly_total_delivery_delay=pl.col("delivery_delay_days")
            .sum()
            .cast(pl.Int16),
            weekly_avg_approval_lag=pl.col("approval_lag_days").mean().cast(pl.Float32),
        )
    )

    seller_dim = lf_filtered.group_by("seller_id_int").agg(
        first_order_date=pl.col("order_date").min(),
        first_order_year_week=pl.col("order_year_week").min(),
    )

    seller_semantic = {
        "seller_weekly_fact": seller_weekly_fact,
        "seller_dim": seller_dim,
    }

    return seller_semantic


# ------------------------------------------------------------
# CUSTOMER SEMANTIC BUILDER
# ------------------------------------------------------------


def build_customer_semantic(lf: pl.LazyFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Customer-centric analytical layer from assembled integer-mapped events.

    Contract:
    - Subtractive Filtering: Selects strictly required columns for performance.
    - Transformation: Derives week_start_date and boolean status flags.
    - Aggregation: Computes weekly performance metrics (revenue, lead times, delays) per customer using optimized Integer keys.
    - Hydration: Loads historical customer dimension table from the assembly zone.

    Optimization Logic:
    - Integer Key Optimization: Utilizes UInt32/UInt64 grouping keys (customer_id_int) to maintain a constant memory profile during non-blocking aggregation.
    - Metric Downcasting: Enforces Int16 for counts/days and Float32 for revenue to minimize row width.

    Invariants:
    - Fact Grain: Strictly 1 row per ('customer_id_int', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'customer_id_int'.

    Outputs:
    - Dict containing 'customer_weekly_fact' (LazyFrame) and 'customer_dim' (LazyFrame).

    Failures:
    - [Structural] Crashes if input LazyFrame lacks required columns.
    - [Operational] Crashes if 'df_customers' cannot be loaded from the assembly zone.
    """

    needed_cols = [
        "customer_id_int",
        "order_year_week",
        "order_date",
        "order_status",
        "order_id_int",
        "order_revenue",
        "lead_time_days",
        "delivery_delay_days",
        "approval_lag_days",
    ]

    lf_filtered = lf.select(needed_cols)

    customer_weekly_fact = (
        lf_filtered.with_columns(
            week_start_date=pl.col("order_date").dt.truncate("1w"),
            is_delivered=pl.col("order_status").eq("delivered"),
            is_cancelled=pl.col("order_status").eq("cancelled"),
        )
        .group_by(["customer_id_int", "order_year_week"])
        .agg(
            week_start_date=pl.col("week_start_date").min(),
            weekly_order_count=pl.col("order_id_int").count().cast(pl.Int16),
            weekly_delivered_orders=pl.col("is_delivered").sum().cast(pl.Int16),
            weekly_cancelled_orders=pl.col("is_cancelled").sum().cast(pl.Int16),
            weekly_revenue=pl.col("order_revenue").sum().cast(pl.Float32),
            weekly_avg_lead_time=pl.col("lead_time_days").mean().cast(pl.Float32),
            weekly_total_lead_time=pl.col("lead_time_days").sum().cast(pl.Int16),
            weekly_avg_delivery_delay=pl.col("delivery_delay_days")
            .mean()
            .cast(pl.Float32),
            weekly_total_delivery_delay=pl.col("delivery_delay_days")
            .sum()
            .cast(pl.Int16),
            weekly_avg_approval_lag=pl.col("approval_lag_days").mean().cast(pl.Float32),
        )
    )

    customer_dim = load_assembled_data(
        base_path=run_context.assembled_path, table_name="df_customers"
    )

    customer_semantic = {
        "customer_weekly_fact": customer_weekly_fact,
        "customer_dim": customer_dim,
    }

    return customer_semantic


# ------------------------------------------------------------
# PRODUCT SEMANTIC BUILDER
# ------------------------------------------------------------


def build_product_semantic(lf: pl.LazyFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Product-centric analytical layer from assembled integer-mapped events.

    Contract:
    - Subtractive Filtering: Selects strictly required columns for performance.
    - Transformation: Derives week_start_date and boolean status flags.
    - Aggregation: Computes weekly performance metrics (revenue, lead times, delays) per product using optimized Integer keys.
    - Hydration: Loads historical product dimension table from the assembly zone.

    Optimization Logic:
    - Integer Key Optimization: Utilizes UInt32/UInt64 grouping keys (product_id_int) to maintain a constant memory profile during non-blocking aggregation.
    - Metric Downcasting: Enforces Int16 for counts/days and Float32 for revenue to minimize row width.

    Invariants:
    - Fact Grain: Strictly 1 row per ('product_id_int', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'product_id_int'.

    Outputs:
    - Dict containing 'product_weekly_fact' (LazyFrame) and 'product_dim' (LazyFrame).

    Failures:
    - [Structural] Crashes if input LazyFrame lacks required columns.
    - [Operational] Crashes if 'df_products' cannot be loaded from the assembly zone.
    """

    needed_cols = [
        "product_id_int",
        "order_year_week",
        "order_date",
        "order_status",
        "order_id_int",
        "order_revenue",
        "lead_time_days",
        "delivery_delay_days",
        "approval_lag_days",
    ]

    lf_filtered = lf.select(needed_cols)

    product_weekly_fact = (
        lf_filtered.with_columns(
            week_start_date=pl.col("order_date").dt.truncate("1w"),
            is_delivered=pl.col("order_status").eq("delivered"),
            is_cancelled=pl.col("order_status").eq("cancelled"),
        )
        .group_by(["product_id_int", "order_year_week"])
        .agg(
            week_start_date=pl.col("week_start_date").min(),
            weekly_order_count=pl.col("order_id_int").count().cast(pl.Int16),
            weekly_delivered_orders=pl.col("is_delivered").sum().cast(pl.Int16),
            weekly_cancelled_orders=pl.col("is_cancelled").sum().cast(pl.Int16),
            weekly_revenue=pl.col("order_revenue").sum().cast(pl.Float32),
            weekly_avg_lead_time=pl.col("lead_time_days").mean().cast(pl.Float32),
            weekly_total_lead_time=pl.col("lead_time_days").sum().cast(pl.Int16),
            weekly_avg_delivery_delay=pl.col("delivery_delay_days")
            .mean()
            .cast(pl.Float32),
            weekly_total_delivery_delay=pl.col("delivery_delay_days")
            .sum()
            .cast(pl.Int16),
            weekly_avg_approval_lag=pl.col("approval_lag_days").mean().cast(pl.Float32),
        )
    )

    product_dim = load_assembled_data(
        base_path=run_context.assembled_path, table_name="df_products"
    )

    product_semantic = {
        "product_weekly_fact": product_weekly_fact,
        "product_dim": product_dim,
    }

    return product_semantic
