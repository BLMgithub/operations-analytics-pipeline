# =============================================================================
# TABLE CONFIGURATIONS
# =============================================================================

# ------------------------------------------------------------
# CONFIGURATIONS FOR validate_raw_data.py
# ------------------------------------------------------------

TABLE_CONFIG = {
    "df_orders": {
        "role": "event_fact",
        "primary_key": ["order_id"],
        "allowed_column": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_timestamp",
            "order_estimated_delivery_date",
        ],
    },
    "df_order_items": {
        "role": "transaction_detail",
        "primary_key": ["order_id"],
        "allowed_column": [
            "order_id",
            "product_id",
            "seller_id",
            "price",
            "shipping_charges",
        ],
    },
    "df_customers": {
        "role": "entity_reference",
        "primary_key": ["customer_id"],
        "allowed_column": [
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    },
    "df_payments": {
        "role": "transaction_detail",
        "primary_key": ["order_id", "payment_sequential"],
        "allowed_column": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    },
    "df_products": {
        "role": "entity_reference",
        "primary_key": ["product_id"],
        "allowed_column": [
            "product_id",
            "product_category_name",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    },
}


# ------------------------------------------------------------
# CONFIGURATIONS FOR apply_raw_data_contract.py
# ------------------------------------------------------------

REQUIRED_TIMESTAMPS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_timestamp",
    "order_estimated_delivery_date",
]


TIMESTAMP_FORMATS = {
    "order_purchase_timestamp": "%Y-%m-%d %H:%M:%S",
    "order_approved_at": "%Y-%m-%d %H:%M:%S",
    "order_delivered_timestamp": "%Y-%m-%d %H:%M:%S",
    "order_estimated_delivery_date": "%Y-%m-%d",
}


# ------------------------------------------------------------
# CONFIGURATIONS FOR assemble_validate_events.py
# ------------------------------------------------------------

# Assemble events enforced schema and dtypes
ASSEMBLE_ENFORCED_SCHEMA = [
    "order_id",
    "order_revenue",
    "seller_id",
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

ASSEMBLE_ENFORCED_DTYPES = {
    "order_id": "string",
    "order_revenue": "float64",
    "seller_id": "string",
    "product_id": "string",
    "order_status": "string",
    "order_purchase_timestamp": "datetime64[ns]",
    "order_approved_at": "datetime64[ns]",
    "order_delivered_timestamp": "datetime64[ns]",
    "lead_time_days": "int64",
    "approval_lag_days": "int64",
    "delivery_delay_days": "int64",
    "order_date": "datetime64[ns]",
    "order_year": "int64",
}


# ------------------------------------------------------------
# CONFIGURATIONS FOR build_bi_semantic_layer.py
# ------------------------------------------------------------


# Seller dimension enforced schema and dtypes
SELLER_DIM_ENFORCED_SCHEMA = [
    "seller_id",
    "first_order_date",
    "first_order_year_week",
    "run_id",
]

SELLER_DIM_ENFORCED_DTYPES = {
    "seller_id": "string",
    "first_order_date": "datetime64[ns]",
    "first_order_year_week": "string",
    "run_id": "string",
}


# Seller Facts enforced schema and dtypes
SELLER_FACT_ENFORCED_SCHEMA = [
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

SELLER_FACT_ENFORCED_DTYPES = {
    "seller_id": "string",
    "order_year_week": "string",
    "week_start_date": "datetime64[ns]",
    "run_id": "string",
    "weekly_order_count": "int64",
    "weekly_delivered_orders": "int64",
    "weekly_cancelled_orders": "int64",
    "weekly_revenue": "float64",
    "weekly_avg_lead_time": "float64",
    "weekly_total_lead_time": "int64",
    "weekly_avg_delivery_delay": "float64",
    "weekly_total_delivery_delay": "int64",
    "weekly_avg_approval_lag": "float64",
}
