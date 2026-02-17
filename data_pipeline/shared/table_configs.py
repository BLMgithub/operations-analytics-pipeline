# =============================================================================
# TABLE CONFIGURATIONS
# =============================================================================

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
