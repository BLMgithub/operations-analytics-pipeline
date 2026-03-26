# =============================================================================
# TABLE CONFIGURATIONS RAW DATA
# =============================================================================

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
        "non_nullable_column": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        ],
        "dtypes": {
            "order_id": "string",
            "customer_id": "string",
            "order_status": "category",
            "order_purchase_timestamp": "datetime64[ns]",
            "order_approved_at": "datetime64[ns]",
            "order_delivered_timestamp": "datetime64[ns]",
            "order_estimated_delivery_date": "datetime64[ns]",
        },
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
        "non_nullable_column": [
            "order_id",
            "product_id",
            "seller_id",
            "price",
        ],
        "dtypes": {
            "order_id": "string",
            "product_id": "string",
            "seller_id": "string",
            "price": "float32",
        },
    },
    "df_customers": {
        "role": "entity_reference",
        "primary_key": ["customer_id"],
        "required_column": [
            "customer_id",
            "customer_state",
            "customer_city",
            "customer_segment",
            "account_creation_date",
        ],
        "non_nullable_column": [
            "customer_id",
            "customer_state",
            "customer_city",
            "customer_segment",
            "account_creation_date",
        ],
        "dtypes": {
            "customer_id": "string",
            "customer_state": "category",
            "customer_city": "category",
            "customer_segment": "category",
            "account_creation_date": "datetime64[ns]",
        },
    },
    "df_payments": {
        "role": "transaction_detail",
        "primary_key": ["order_id", "payment_sequential"],
        "required_column": [
            "order_id",
            "payment_value",
            "payment_sequential",
        ],
        "non_nullable_column": [
            "order_id",
            "payment_value",
            "payment_sequential",
        ],
        "dtypes": {
            "order_id": "string",
            "payment_value": "float32",
            "payment_sequential": "int8",
        },
    },
    "df_products": {
        "role": "entity_reference",
        "primary_key": ["product_id"],
        "required_column": [
            "product_id",
            "product_category_name",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_fragility_index",
            "product_weight_g",
            "supplier_tier",
        ],
        "non_nullable_column": [
            "product_id",
            "product_category_name",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_fragility_index",
            "product_weight_g",
            "supplier_tier",
        ],
        "dtypes": {
            "product_id": "string",
            "product_category_name": "category",
            "product_length_cm": "float32",
            "product_height_cm": "float32",
            "product_width_cm": "float32",
            "product_fragility_index": "category",
            "product_weight_g": "float32",
            "supplier_tier": "category",
        },
    },
}


# ------------------------------------------------------------
# CONFIGURATIONS FOR Contract Stage
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
