# =============================================================================
# Table configuration for Validation and Contract stage
# =============================================================================

import polars as pl

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
            "order_id": pl.String,
            "customer_id": pl.String,
            "order_status": pl.Categorical,
            "order_purchase_timestamp": pl.Datetime(time_unit="us"),
            "order_approved_at": pl.Datetime(time_unit="us"),
            "order_delivered_timestamp": pl.Datetime(time_unit="us"),
            "order_estimated_delivery_date": pl.Datetime(time_unit="us"),
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
            "order_id": pl.String,
            "product_id": pl.String,
            "seller_id": pl.String,
            "price": pl.Float32,
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
            "customer_id": pl.String,
            "customer_state": pl.Categorical,
            "customer_city": pl.Categorical,
            "customer_segment": pl.Categorical,
            "account_creation_date": pl.Datetime(time_unit="us"),
        },
    },
    "df_payments": {
        "role": "transaction_detail",
        "primary_key": ["order_id"],
        "required_column": [
            "order_id",
            "payment_value",
        ],
        "non_nullable_column": [
            "order_id",
            "payment_value",
        ],
        "dtypes": {
            "order_id": pl.String,
            "payment_value": pl.Float32,
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
            "product_id": pl.String,
            "product_category_name": pl.Categorical,
            "product_length_cm": pl.Float32,
            "product_height_cm": pl.Float32,
            "product_width_cm": pl.Float32,
            "product_fragility_index": pl.Categorical,
            "product_weight_g": pl.Float32,
            "supplier_tier": pl.Categorical,
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
