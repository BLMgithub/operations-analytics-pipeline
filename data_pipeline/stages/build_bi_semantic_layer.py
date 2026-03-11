# =============================================================================
# BUILD BI-TOOL SEMANTIC LAYER
# =============================================================================
# - Produce contract-compliant facts and dimensions safe for direct BI consumption
# - Define and lock analytical grains for consistent aggregation and reporting
# - Enforce referential integrity between fact and dimension tables

import pandas as pd
from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.loader_exporter import load_logical_table, export_file
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
    Build seller weekly semantic layer from assembled events.

    Fact grain:
    - 1 row per (seller_id, order_year_week)

    Dimension grain:
    - 1 row per seller_id

    Behavior:
    - Enforce single run_id lineage
    - Derive ISO week alignment
    - Aggregate event metrics to seller-week

    Returns:
    - Aggregated fact dataframe
    - Seller dimension dataframe
    """

    read_assembled = df.copy()

    if read_assembled["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    read_assembled["week_start_date"] = (
        read_assembled["order_date"].dt.to_period("W-MON").dt.start_time
    )
    read_assembled["is_delivered"] = read_assembled["order_status"].eq("delivered")
    read_assembled["is_cancelled"] = read_assembled["order_status"].eq("cancelled")

    seller_weekly_fact = read_assembled.groupby(
        ["seller_id", "order_year_week"],
        as_index=False,
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

    seller_dim = read_assembled.groupby(
        "seller_id",
        as_index=False,
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
    Build customer weekly semantic layer from assembled events.

    Fact grain:
    - 1 row per (customer_id, order_year_week)

    Dimension grain:
    - 1 row per customer_id

    Behavior:
    - Enforce single run_id lineage
    - Derive ISO week alignment
    - Aggregate event metrics to customer-week

    Returns:
    - Aggregated fact dataframe
    - customer dimension dataframe
    """

    read_assembled = df.copy()

    if read_assembled["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    read_assembled["week_start_date"] = (
        read_assembled["order_date"].dt.to_period("W-MON").dt.start_time
    )
    read_assembled["is_delivered"] = read_assembled["order_status"].eq("delivered")
    read_assembled["is_cancelled"] = read_assembled["order_status"].eq("cancelled")

    customer_weekly_fact = read_assembled.groupby(
        ["customer_id", "order_year_week"],
        as_index=False,
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

    df_customer = load_logical_table(run_context.contracted_path, "df_customers")

    if df_customer is None or df_customer.empty:
        raise RuntimeError(
            "build_customer_semantic: df_customers logical table missing or empty"
        )

    customer_dim = (
        df_customer[["customer_id", "customer_state"]]
        .drop_duplicates(subset=["customer_id"])
        .copy()
    )

    if customer_dim["customer_id"].duplicated().any():
        raise RuntimeError(
            "build_customer_semantic: Duplicate customer_id detected in customer_dim"
        )

    customer_dim["run_id"] = read_assembled["run_id"].iloc[0]

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
    Build product weekly semantic layer from assembled events.

    Fact grain:
    - 1 row per (product_id, order_year_week)

    Dimension grain:
    - 1 row per product_id

    Behavior:
    - Enforce single run_id lineage
    - Derive ISO week alignment
    - Aggregate event metrics to product-week

    Returns:
    - Aggregated fact dataframe
    - product dimension dataframe
    """

    read_assembled = df.copy()

    if read_assembled["run_id"].nunique() != 1:
        raise RuntimeError("Multiple run_ids detected")

    read_assembled["week_start_date"] = (
        read_assembled["order_date"].dt.to_period("W-MON").dt.start_time
    )
    read_assembled["is_delivered"] = read_assembled["order_status"].eq("delivered")
    read_assembled["is_cancelled"] = read_assembled["order_status"].eq("cancelled")

    product_weekly_fact = read_assembled.groupby(
        ["product_id", "order_year_week"],
        as_index=False,
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

    df_products = load_logical_table(run_context.contracted_path, "df_products")

    if df_products is None or df_products.empty:
        raise RuntimeError(
            "build_product_semantic: df_products logical table missing or empty"
        )

    product_dim = (
        df_products[
            [
                "product_id",
                "product_category_name",
                "product_weight_g",
            ]
        ]
        .drop_duplicates(subset=["product_id"])
        .copy()
    )

    product_dim["run_id"] = read_assembled["run_id"].iloc[0]

    product_semantic = {
        "product_weekly_fact": product_weekly_fact,
        "product_dim": product_dim,
    }

    return product_semantic


# ------------------------------------------------------------
# SEMANTIC BUILDING CONTRACTS
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# BUILD BI SEMANTIC
# ------------------------------------------------------------


def build_semantic_layer(run_context: RunContext) -> Dict:
    """
    Builds semantic modules from the assembled event layer and
    exports contract-compliant BI artifacts.

    Execution:
    - Load assembled event dataset (order grain)
    - Fail fast if dataset is missing or empty
    - Execute registered semantic builders
    - Apply module-level freeze contracts
        - schema enforcement
        - dtype enforcement
        - grain validation
        - deterministic ordering
    - Export semantic artifacts into run-scoped semantic directory
    - Aggregate findings into report

    Guarantees:
    - Each semantic module enforces its declared grain
    - All exported tables are contract-compliant
    - No decision logic embedded

    Failure:
    - Any module failure halts semantic stage
    """

    report = {
        "status": "success",
        "steps": {
            "load_tables": init_report(),
        },
        "modules": {},
    }

    def fail_module(module_name, table_name=None):
        report["status"] = "failed"
        report["failed_module"] = module_name

        if table_name:
            report["failed_table"] = table_name

        return report

    assembled_path = run_context.assembled_path

    # Load table
    load_report = report["steps"]["load_tables"]

    df_assembled = load_logical_table(
        assembled_path,
        "assembled_events",
        log_info=lambda msg: log_info(msg, load_report),
        log_error=lambda msg: log_error(msg, load_report),
    )

    if df_assembled is None or df_assembled.empty:
        log_error("assembled_events logical table missing or empty", load_report)

        report["status"] = "failed"
        report["failed_step"] = "load_tables"

        return report

    for module_name, module in SEMANTIC_MODULES.items():

        # Module level report
        module_report = init_report()
        report["modules"][module_name] = module_report

        # Execute module builder
        try:
            builder_output = module["builder"](df_assembled, run_context)

        except Exception as e:
            log_error(str(e), module_report)

            module_report["status"] = "failed"
            return fail_module(module_name)

        semantic_module_path = run_context.semantic_path / module_name

        # builders return dict {table_name: df}
        for table_name, df in builder_output.items():

            # Table level report
            table_report = init_report()
            module_report[table_name] = table_report

            # Validate builder output
            if table_name not in module["tables"]:
                log_error(f"Unexpected table returned: {table_name}", module_report)
                module_report["status"] = "failed"

                return fail_module(module_name, table_name)

            meta = module["tables"][table_name]

            try:
                # Validate duplicates
                if df.duplicated(meta["grain"]).any():
                    raise RuntimeError(f"Duplicates in {meta['grain']}")

                # Validate required columns
                missing = set(meta["schema"]) - set(df.columns)
                if missing:
                    raise RuntimeError(f"Missing required columns: {missing}")

            except Exception as e:
                log_error(str(e), table_report)

                table_report["status"] = "failed"
                module_report["status"] = "failed"

                return fail_module(module_name, table_name)

            # Enforce dtypes
            df = df[meta["schema"]].astype(meta["dtypes"])

            # Deterministic sort
            df = df.sort_values(meta["grain"]).reset_index(drop=True)

            year = run_context.run_id[:4]
            month = run_context.run_id[4:6]

            filename = f"{table_name}_{year}_{month}.parquet"
            output_path = semantic_module_path / filename

            if not export_file(df, output_path):
                log_error("Export failed", table_report)

                table_report["status"] = "failed"
                module_report["status"] = "failed"

                return fail_module(module_name, table_name)

            log_info(
                f"Export success: {filename} ({len(df)} rows)",
                table_report,
            )

    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
