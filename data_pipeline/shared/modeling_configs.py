# =============================================================================
# Table modeling configurations for Assemble and Semantic stage
# =============================================================================

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

ASSEMBLE_DTYPES = {
    "order_id": "string",
    "order_revenue": "float32",
    "seller_id": "string",
    "customer_id": "string",
    "product_id": "string",
    "order_status": "category",
    "order_purchase_timestamp": "datetime64[ns]",
    "order_approved_at": "datetime64[ns]",
    "order_delivered_timestamp": "datetime64[ns]",
    "lead_time_days": "int16",
    "approval_lag_days": "int16",
    "delivery_delay_days": "int16",
    "order_date": "datetime64[ns]",
    "order_year": "int16",
    "order_year_week": "string",
    "run_id": "string",
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

SELLER_DIM_DTYPES = {
    "seller_id": "string",
    "first_order_date": "datetime64[ns]",
    "first_order_year_week": "string",
    "run_id": "string",
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

SELLER_FACT_DTYPES = {
    "seller_id": "string",
    "order_year_week": "string",
    "week_start_date": "datetime64[ns]",
    "run_id": "string",
    "weekly_order_count": "int16",
    "weekly_delivered_orders": "int16",
    "weekly_cancelled_orders": "int16",
    "weekly_revenue": "float32",
    "weekly_avg_lead_time": "float32",
    "weekly_total_lead_time": "int32",
    "weekly_avg_delivery_delay": "float32",
    "weekly_total_delivery_delay": "int32",
    "weekly_avg_approval_lag": "float32",
}

# ------------------------------------------------------------
# CUSTOMER SEMANTIC CONFIGS
# ------------------------------------------------------------

# Customer Dimension and dtypes
CUSTOMER_DIM_SCHEMA = [
    "customer_id",
    "customer_state",
    "run_id",
]


CUSTOMER_DIM_DTYPES = {
    "customer_id": "string",
    "customer_state": "string",
    "run_id": "string",
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


CUSTOMER_FACT_DTYPES = {
    "customer_id": "string",
    "order_year_week": "string",
    "week_start_date": "datetime64[ns]",
    "run_id": "string",
    "weekly_order_count": "int16",
    "weekly_delivered_orders": "int16",
    "weekly_cancelled_orders": "int16",
    "weekly_revenue": "float32",
    "weekly_avg_lead_time": "float32",
    "weekly_total_lead_time": "int32",
    "weekly_avg_delivery_delay": "float32",
    "weekly_total_delivery_delay": "int32",
    "weekly_avg_approval_lag": "float32",
}


# ------------------------------------------------------------
# PRODUCT SEMANTIC CONFIGS
# ------------------------------------------------------------

# Product Dim and dtypes
PRODUCT_DIM_SCHEMA = [
    "product_id",
    "product_category_name",
    "product_weight_g",
    "run_id",
]

PRODUCT_DIM_DTYPES = {
    "product_id": "string",
    "product_category_name": "string",
    "product_weight_g": "float",
    "run_id": "string",
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


PRODUCT_FACT_DTYPES = {
    "product_id": "string",
    "order_year_week": "string",
    "week_start_date": "datetime64[ns]",
    "run_id": "string",
    "weekly_order_count": "int16",
    "weekly_delivered_orders": "int16",
    "weekly_cancelled_orders": "int16",
    "weekly_revenue": "float32",
    "weekly_avg_lead_time": "float32",
    "weekly_total_lead_time": "int32",
    "weekly_avg_delivery_delay": "float32",
    "weekly_total_delivery_delay": "int32",
    "weekly_avg_approval_lag": "float32",
}
