# =============================================================================
# TABLE CONFIGURATIONS RAW DATA
# =============================================================================

# ------------------------------------------------------------
# CONFIGURATIONS FOR validate_raw_data.py
# ------------------------------------------------------------

TABLE_CONFIG = {
    "df_orders": {
        "role": "event_fact",
        "primary_key": ["order_id"],
        "required_column": [
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
        "required_column": [
            "order_id",
            "product_id",
            "seller_id",
            "price",
        ],
    },
    "df_customers": {
        "role": "entity_reference",
        "primary_key": ["customer_id"],
        "required_column": [
            "customer_id",
            "customer_state",
        ],
    },
    "df_payments": {
        "role": "transaction_detail",
        "primary_key": ["order_id", "payment_sequential"],
        "required_column": [
            "order_id",
            "payment_value",
        ],
    },
    "df_products": {
        "role": "entity_reference",
        "primary_key": ["product_id"],
        "required_column": [
            "product_id",
            "product_category_name",
            "product_weight_g",
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
