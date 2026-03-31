# =============================================================================
# Table modeling configurations for Assemble and Semantic stage
# =============================================================================

import polars as pl
from typing import Mapping
from data_pipeline.shared.table_configs import TABLE_CONFIG

# ------------------------------------------------------------
# ASSEMBLE EVENTS CONFIGS
# ------------------------------------------------------------

# Assemble events enforced schema and dtypes
ASSEMBLE_SCHEMA = [
    "order_id",
    "order_revenue",
    "seller_id",
    "customer_id",
    "product_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_timestamp",
    "lead_time_days",
    "approval_lag_days",
    "delivery_delay_days",
    "order_date",
    "order_year",
    "order_year_week",
    "run_id",
]

ASSEMBLE_DTYPES: Mapping[str, pl.DataType] = {
    "order_id": pl.String(),
    "order_revenue": pl.Float32(),
    "seller_id": pl.String(),
    "customer_id": pl.String(),
    "product_id": pl.String(),
    "order_status": pl.Categorical(),
    "order_purchase_timestamp": pl.Datetime(),
    "order_approved_at": pl.Datetime(),
    "order_delivered_timestamp": pl.Datetime(),
    "lead_time_days": pl.Int16(),
    "approval_lag_days": pl.Int16(),
    "delivery_delay_days": pl.Int16(),
    "order_date": pl.Datetime(),
    "order_year": pl.Int16(),
    "order_year_week": pl.String(),
    "run_id": pl.String(),
}

#
#
#

dimension_table = ["df_customers", "df_products"]
DIMENSION_REFERENCES = {
    table: {
        "primary_key": TABLE_CONFIG[table]["primary_key"],
        "required_column": TABLE_CONFIG[table]["required_column"],
    }
    for table in dimension_table
}


# ------------------------------------------------------------
# SELLER SEMANTIC CONFIGS
# ------------------------------------------------------------

# Seller dimension enforced schema and dtypes
SELLER_DIM_SCHEMA = [
    "seller_id",
    "first_order_date",
    "first_order_year_week",
    "run_id",
]

SELLER_DIM_DTYPES: Mapping[str, pl.DataType] = {
    "seller_id": pl.String(),
    "first_order_date": pl.Datetime(),
    "first_order_year_week": pl.String(),
    "run_id": pl.String(),
}


# Seller Facts enforced schema and dtypes
SELLER_FACT_SCHEMA = [
    "seller_id",
    "order_year_week",
    "week_start_date",
    "run_id",
    "weekly_order_count",
    "weekly_delivered_orders",
    "weekly_cancelled_orders",
    "weekly_revenue",
    "weekly_avg_lead_time",
    "weekly_total_lead_time",
    "weekly_avg_delivery_delay",
    "weekly_total_delivery_delay",
    "weekly_avg_approval_lag",
]

SELLER_FACT_DTYPES: Mapping[str, pl.DataType] = {
    "seller_id": pl.String(),
    "order_year_week": pl.String(),
    "week_start_date": pl.Datetime(),
    "run_id": pl.String(),
    "weekly_order_count": pl.Int16(),
    "weekly_delivered_orders": pl.Int16(),
    "weekly_cancelled_orders": pl.Int16(),
    "weekly_revenue": pl.Float32(),
    "weekly_avg_lead_time": pl.Float32(),
    "weekly_total_lead_time": pl.Int32(),
    "weekly_avg_delivery_delay": pl.Float32(),
    "weekly_total_delivery_delay": pl.Int32(),
    "weekly_avg_approval_lag": pl.Float32(),
}


# ------------------------------------------------------------
# CUSTOMER SEMANTIC CONFIGS
# ------------------------------------------------------------

# Customer Dimension and dtypes
CUSTOMER_DIM_SCHEMA = [
    "customer_id",
    "customer_state",
    "customer_city",
    "customer_segment",
    "account_creation_date",
]

CUSTOMER_DIM_DTYPES: Mapping[str, pl.DataType] = {
    "customer_id": pl.String(),
    "customer_state": pl.Categorical(),
    "customer_city": pl.Categorical(),
    "customer_segment": pl.Categorical(),
    "account_creation_date": pl.Datetime(),
}

# Customer Fact and dtypes
CUSTOMER_FACT_SCHEMA = [
    "customer_id",
    "order_year_week",
    "week_start_date",
    "run_id",
    "weekly_order_count",
    "weekly_delivered_orders",
    "weekly_cancelled_orders",
    "weekly_revenue",
    "weekly_avg_lead_time",
    "weekly_total_lead_time",
    "weekly_avg_delivery_delay",
    "weekly_total_delivery_delay",
    "weekly_avg_approval_lag",
]

CUSTOMER_FACT_DTYPES: Mapping[str, pl.DataType] = {
    "customer_id": pl.String(),
    "order_year_week": pl.String(),
    "week_start_date": pl.Datetime(),
    "run_id": pl.String(),
    "weekly_order_count": pl.Int16(),
    "weekly_delivered_orders": pl.Int16(),
    "weekly_cancelled_orders": pl.Int16(),
    "weekly_revenue": pl.Float32(),
    "weekly_avg_lead_time": pl.Float32(),
    "weekly_total_lead_time": pl.Int32(),
    "weekly_avg_delivery_delay": pl.Float32(),
    "weekly_total_delivery_delay": pl.Int32(),
    "weekly_avg_approval_lag": pl.Float32(),
}


# ------------------------------------------------------------
# PRODUCT SEMANTIC CONFIGS
# ------------------------------------------------------------

# Product Dim and dtypes
PRODUCT_DIM_SCHEMA = [
    "product_id",
    "product_category_name",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_fragility_index",
    "product_weight_g",
    "supplier_tier",
]

PRODUCT_DIM_DTYPES: Mapping[str, pl.DataType] = {
    "product_id": pl.String(),
    "product_category_name": pl.Categorical(),
    "product_length_cm": pl.Float32(),
    "product_height_cm": pl.Float32(),
    "product_width_cm": pl.Float32(),
    "product_fragility_index": pl.Categorical(),
    "product_weight_g": pl.Float32(),
    "supplier_tier": pl.Categorical(),
}


# Product Fact and dtypes
PRODUCT_FACT_SCHEMA = [
    "product_id",
    "order_year_week",
    "week_start_date",
    "run_id",
    "weekly_order_count",
    "weekly_delivered_orders",
    "weekly_cancelled_orders",
    "weekly_revenue",
    "weekly_avg_lead_time",
    "weekly_total_lead_time",
    "weekly_avg_delivery_delay",
    "weekly_total_delivery_delay",
    "weekly_avg_approval_lag",
]


PRODUCT_FACT_DTYPES: Mapping[str, pl.DataType] = {
    "product_id": pl.String(),
    "order_year_week": pl.String(),
    "week_start_date": pl.Datetime(),
    "run_id": pl.String(),
    "weekly_order_count": pl.Int16(),
    "weekly_delivered_orders": pl.Int16(),
    "weekly_cancelled_orders": pl.Int16(),
    "weekly_revenue": pl.Float32(),
    "weekly_avg_lead_time": pl.Float32(),
    "weekly_total_lead_time": pl.Int32(),
    "weekly_avg_delivery_delay": pl.Float32(),
    "weekly_total_delivery_delay": pl.Int32(),
    "weekly_avg_approval_lag": pl.Float32(),
}
