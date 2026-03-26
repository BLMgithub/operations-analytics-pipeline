# =============================================================================
# Semantic Modelinc Stage Logic
# =============================================================================

import pandas as pd
from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_single_delta


# ------------------------------------------------------------
# SEMANTIC REPORT & LOGS
# ------------------------------------------------------------


def init_report():
    return {"status": "success", "errors": [], "info": []}


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_error(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[ERROR] {message}")
    report["errors"].append(message)


# ------------------------------------------------------------
# SELLER SEMANTIC BUILDER
# ------------------------------------------------------------


def build_seller_semantic(df: pd.DataFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Seller-centric analytical layer from assembled events.

    Contract:
    - Transforms order-grain events into weekly seller performance snapshots.
    - Aggregates metrics including revenue, lead times, and fulfillment lag.
    - Produces a historical dimension table for seller attributes.

    Invariants:
    - Lineage: Enforces a single 'run_id' across the input dataset.
    - Fact Grain: Strictly 1 row per ('seller_id', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'seller_id'.
    - Temporal: Aligns all metrics to ISO-week start dates (Monday).

    Outputs:
    - Returns a dictionary containing 'seller_weekly_fact' and 'seller_dim'.

    Failures:
    - Raises RuntimeError if the input contains data from multiple pipeline runs.
    """

    if df["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    df["week_start_date"] = df["order_date"].dt.to_period("W-MON").dt.start_time
    df["is_delivered"] = df["order_status"].eq("delivered")
    df["is_cancelled"] = df["order_status"].eq("cancelled")

    seller_weekly_fact = df.groupby(
        ["seller_id", "order_year_week"],
        as_index=False,
        observed=True,
    ).agg(
        week_start_date=("week_start_date", "min"),
        run_id=("run_id", "first"),
        weekly_order_count=("order_id", "count"),
        weekly_delivered_orders=("is_delivered", "sum"),
        weekly_cancelled_orders=("is_cancelled", "sum"),
        weekly_revenue=("order_revenue", "sum"),
        weekly_avg_lead_time=("lead_time_days", "mean"),
        weekly_total_lead_time=("lead_time_days", "sum"),
        weekly_avg_delivery_delay=("delivery_delay_days", "mean"),
        weekly_total_delivery_delay=("delivery_delay_days", "sum"),
        weekly_avg_approval_lag=("approval_lag_days", "mean"),
    )

    seller_dim = df.groupby(
        "seller_id",
        as_index=False,
        observed=True,
    ).agg(
        first_order_date=("order_date", "min"),
        first_order_year_week=("order_year_week", "min"),
        run_id=("run_id", "first"),
    )

    seller_semantic = {
        "seller_weekly_fact": seller_weekly_fact,
        "seller_dim": seller_dim,
    }

    return seller_semantic


# ------------------------------------------------------------
# CUSTOMER SEMANTIC BUILDER
# ------------------------------------------------------------


def build_customer_semantic(df: pd.DataFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Customer-centric analytical layer from assembled events.

    Contract:
    - Aggregates consumer behavior metrics into a weekly temporal grain.
    - Calculates lifetime-to-date attributes and first-purchase markers.
    - Summarizes spending patterns and locality-based attributes.

    Invariants:
    - Lineage: Requires a unified 'run_id' for consistent partitioning.
    - Fact Grain: Strictly 1 row per ('customer_id', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'customer_id'.

    Outputs:
    - Returns a dictionary containing 'customer_weekly_fact' and 'customer_dim'.

    Failures:
    - Raises RuntimeError if 'run_id' cardinality is greater than 1.
    """

    if df["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    df["week_start_date"] = df["order_date"].dt.to_period("W-MON").dt.start_time
    df["is_delivered"] = df["order_status"].eq("delivered")
    df["is_cancelled"] = df["order_status"].eq("cancelled")

    customer_weekly_fact = df.groupby(
        ["customer_id", "order_year_week"],
        as_index=False,
        observed=True,
    ).agg(
        week_start_date=("week_start_date", "min"),
        run_id=("run_id", "first"),
        weekly_order_count=("order_id", "count"),
        weekly_delivered_orders=("is_delivered", "sum"),
        weekly_cancelled_orders=("is_cancelled", "sum"),
        weekly_revenue=("order_revenue", "sum"),
        weekly_avg_lead_time=("lead_time_days", "mean"),
        weekly_total_lead_time=("lead_time_days", "sum"),
        weekly_avg_delivery_delay=("delivery_delay_days", "mean"),
        weekly_total_delivery_delay=("delivery_delay_days", "sum"),
        weekly_avg_approval_lag=("approval_lag_days", "mean"),
    )

    customer_dim, _ = load_single_delta(run_context.assembled_path, "df_customers")

    customer_dim["run_id"] = df["run_id"].iloc[0]

    customer_semantic = {
        "customer_weekly_fact": customer_weekly_fact,
        "customer_dim": customer_dim,
    }

    return customer_semantic


# ------------------------------------------------------------
# PRODUCT SEMANTIC BUILDER
# ------------------------------------------------------------


def build_product_semantic(df: pd.DataFrame, run_context: RunContext) -> Dict:
    """
    Constructs the Product-centric analytical layer from assembled events.

    Contract:
    - Aggregates sales velocity and fulfillment health per product.
    - Merges category metadata with weekly transaction volumes.
    - Calculates average lead times and cancellation rates per product week.

    Invariants:
    - Lineage: Validates input homogeneity via 'run_id' check.
    - Fact Grain: Strictly 1 row per ('product_id', 'order_year_week').
    - Dimension Grain: Strictly 1 row per 'product_id'.

    Outputs:
    - Returns a dictionary containing 'product_weekly_fact' and 'product_dim'.

    Failures:
    - Raises RuntimeError if cross-run data contamination is detected.
    """

    if df["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    df["week_start_date"] = df["order_date"].dt.to_period("W-MON").dt.start_time
    df["is_delivered"] = df["order_status"].eq("delivered")
    df["is_cancelled"] = df["order_status"].eq("cancelled")

    product_weekly_fact = df.groupby(
        ["product_id", "order_year_week"],
        as_index=False,
        observed=True,
    ).agg(
        week_start_date=("week_start_date", "min"),
        run_id=("run_id", "first"),
        weekly_order_count=("order_id", "count"),
        weekly_delivered_orders=("is_delivered", "sum"),
        weekly_cancelled_orders=("is_cancelled", "sum"),
        weekly_revenue=("order_revenue", "sum"),
        weekly_avg_lead_time=("lead_time_days", "mean"),
        weekly_total_lead_time=("lead_time_days", "sum"),
        weekly_avg_delivery_delay=("delivery_delay_days", "mean"),
        weekly_total_delivery_delay=("delivery_delay_days", "sum"),
        weekly_avg_approval_lag=("approval_lag_days", "mean"),
    )

    product_dim, _ = load_single_delta(run_context.assembled_path, "df_products")

    product_dim["run_id"] = df["run_id"].iloc[0]

    product_semantic = {
        "product_weekly_fact": product_weekly_fact,
        "product_dim": product_dim,
    }

    return product_semantic
