# =============================================================================
# Semantic Modeling Stage Registry
# =============================================================================

from data_pipeline.semantic.semantic_logic import (
    build_seller_semantic,
    build_customer_semantic,
    build_product_semantic,
)
from data_pipeline.shared.modeling_configs import (
    SELLER_FACT_SCHEMA,
    SELLER_FACT_DTYPES,
    SELLER_DIM_SCHEMA,
    SELLER_DIM_DTYPES,
    CUSTOMER_FACT_SCHEMA,
    CUSTOMER_FACT_DTYPES,
    CUSTOMER_DIM_SCHEMA,
    CUSTOMER_DIM_DTYPES,
    PRODUCT_FACT_SCHEMA,
    PRODUCT_FACT_DTYPES,
    PRODUCT_DIM_SCHEMA,
    PRODUCT_DIM_DTYPES,
)


SEMANTIC_MODULES = {
    "seller_semantic": {
        "builder": build_seller_semantic,
        "tables": {
            "seller_weekly_fact": {
                "type": "fact",
                "grain": ["seller_id", "order_year_week"],
                "schema": SELLER_FACT_SCHEMA,
                "dtypes": SELLER_FACT_DTYPES,
            },
            "seller_dim": {
                "type": "dim",
                "grain": ["seller_id"],
                "schema": SELLER_DIM_SCHEMA,
                "dtypes": SELLER_DIM_DTYPES,
            },
        },
    },
    "customer_semantic": {
        "builder": build_customer_semantic,
        "tables": {
            "customer_weekly_fact": {
                "type": "fact",
                "grain": ["customer_id", "order_year_week"],
                "schema": CUSTOMER_FACT_SCHEMA,
                "dtypes": CUSTOMER_FACT_DTYPES,
            },
            "customer_dim": {
                "type": "dim",
                "grain": ["customer_id"],
                "schema": CUSTOMER_DIM_SCHEMA,
                "dtypes": CUSTOMER_DIM_DTYPES,
            },
        },
    },
    "product_semantic": {
        "builder": build_product_semantic,
        "tables": {
            "product_weekly_fact": {
                "type": "fact",
                "grain": ["product_id", "order_year_week"],
                "schema": PRODUCT_FACT_SCHEMA,
                "dtypes": PRODUCT_FACT_DTYPES,
            },
            "product_dim": {
                "type": "dim",
                "grain": ["product_id"],
                "schema": PRODUCT_DIM_SCHEMA,
                "dtypes": PRODUCT_DIM_DTYPES,
            },
        },
    },
}
