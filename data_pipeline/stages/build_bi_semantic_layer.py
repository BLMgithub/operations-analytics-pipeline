# =============================================================================
# BUILD BI-TOOL SEMANTIC LAYER
# =============================================================================
# - Produce contract-compliant facts and dimensions safe for direct BI consumption
# - Define and lock analytical grains for consistent aggregation and reporting
# - Enforce referential integrity between fact and dimension tables

import pandas as pd
from typing import Dict, List, Tuple, Literal
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.raw_loader_exporter import load_logical_table, export_file


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
# SELLER WEEKLY SEMANTIC MODELING AND SCHEMA ENFORCEMENT
# ------------------------------------------------------------


def seller_weekly_semantic(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Seller weekly semantic builder.

    Transforms the assembled event table into seller-level weekly
    performance fact and supporting seller dimension.

    Transform behavior:

    - Validates single-run lineage via `run_id`
    - Derives weekly alignment fields and status flags
    - Aggregates event data to seller-week grain
    - Builds seller dimension from first observed activity

    Fact grain:
    - One row per (seller_id, order_year_week)

    Dimension grain:
    - One row per seller_id
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

    return seller_weekly_fact, seller_dim


def freeze_seller_semantic(
    df: pd.DataFrame,
    table_type: Literal["fact", "dim"],
) -> pd.DataFrame:
    """
    Seller semantic contract enforcer.

    Routes the input table to the appropriate fact or dimension
    contract freezer and enforces grain integrity before projection.

    Behavior:

    - Validates `table_type` selector
    - Applies grain-level duplicate checks:
      - fact: (seller_id, order_year_week)
      - dim: seller_id
    - Dispatches to the corresponding schema freezer
    - Returns a BI-ready, schema-stable dataframe
    """

    if table_type not in {"fact", "dim"}:
        raise ValueError

    def freeze_seller_fact(df: pd.DataFrame) -> pd.DataFrame:
        """
        Seller weekly fact contract enforcement.

        Projects the aggregated seller-week dataset into the approved
        fact schema, enforces dtypes, and applies deterministic ordering.

        Enforcement actions:

        - Validates presence of all required fact columns
        - Projects to the contract column order
        - Casts fields to enforced dtypes
        - Sorts by (seller_id, order_year_week)
        - Resets index for clean downstream consumption

        """

        fact_contract = df.copy()

        FACT_SCHEMA = [
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

        FACT_ENFORCED_DTYPES = {
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

        missing_cols = set(FACT_SCHEMA) - set(fact_contract.columns)
        if missing_cols:
            raise RuntimeError(
                f"seller_weekly_fact missing required column(s): {sorted(missing_cols)}"
            )

        fact_contract = fact_contract[FACT_SCHEMA].copy()
        fact_contract = fact_contract.astype(FACT_ENFORCED_DTYPES)
        fact_contract = fact_contract.sort_values(
            ["seller_id", "order_year_week"]
        ).reset_index(drop=True)

        return fact_contract

    def freeze_seller_dim(df: pd.DataFrame) -> pd.DataFrame:
        """
        Seller dimension contract enforcement.

        Projects the seller dimension into the approved schema,
        enforces dtypes, and applies deterministic ordering.

        Enforcement actions:

        - Validates presence of all required dimension columns
        - Projects to the contract column order
        - Casts fields to enforced dtypes
        - Sorts by seller_id
        - Resets index for clean downstream consumption
        """

        dim_contract = df.copy()

        DIM_SCHEMA = [
            "seller_id",
            "first_order_date",
            "first_order_year_week",
            "run_id",
        ]

        DIM_ENFORCED_DTYPES = {
            "seller_id": "string",
            "first_order_date": "datetime64[ns]",
            "first_order_year_week": "string",
            "run_id": "string",
        }

        missing_cols = set(DIM_SCHEMA) - set(dim_contract.columns)
        if missing_cols:
            raise RuntimeError(
                f"seller_dim missing required column(s): {sorted(missing_cols)}"
            )

        dim_contract = dim_contract[DIM_SCHEMA].copy()
        dim_contract = dim_contract.astype(DIM_ENFORCED_DTYPES)
        dim_contract = dim_contract.sort_values("seller_id").reset_index(drop=True)

        return dim_contract

    if table_type == "fact":
        if df.duplicated(["seller_id", "order_year_week"]).any():
            raise RuntimeError("Duplicate seller_id - order_year_week in fact")

        seller_fact_contracted = freeze_seller_fact(df)

        return seller_fact_contracted

    else:
        if df["seller_id"].duplicated().any():
            raise RuntimeError("Duplicate seller_id in dimension")

        seller_dim_contracted = freeze_seller_dim(df)

        return seller_dim_contracted


# ------------------------------------------------------------
# BUILD BI SEMANTIC
# ------------------------------------------------------------


def build_semantic_layer(run_context: RunContext) -> Dict:
    """
    Semantic layer orchestrator.

    Builds seller performance semantic tables from the assembled
    event layer and exports contract-compliant BI artifacts.

    Chronological behavior:

    - Initializes run-scoped reporting and logging helpers.
    - Loads the assembled_events logical table.
    - Fails fast if the assembled dataset is missing or empty.
    - Executes semantic pipeline:
      - seller_weekly_semantic (aggregation)
      - freeze_seller_semantic (fact and dimension contracts)
    - Generates run-partitioned output filenames.
    - Exports semantic tables to the run-scoped semantic directory.
    - Aggregates all findings into the returned report.
    """

    report = init_report()

    def info(msg):
        log_info(msg, report)

    def error(msg):
        log_error(msg, report)

    assembled_path = run_context.assembled_path

    df = load_logical_table(
        assembled_path,
        "assembled_events",
        log_info=info,
        log_error=error,
    )

    if df is None or df.empty:
        error("assembled_events logical table missing or empty")
        report["status"] = "failed"

        return report

    try:
        fact_seller, dim_seller = seller_weekly_semantic(df)
        seller_fact_contracted = freeze_seller_semantic(fact_seller, "fact")
        seller_dim_contracted = freeze_seller_semantic(dim_seller, "dim")

    except Exception as e:
        error(str(e))
        report["status"] = "failed"

        return report

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]

    seller_semantic_tables = {
        f"seller_week_performance_fact_{year}_{month}.parquet": seller_fact_contracted,
        f"dim_seller_{year}_{month}.parquet": seller_dim_contracted,
    }

    for table_name, table in seller_semantic_tables.items():

        output_path = run_context.semantic_path / table_name

        if not export_file(table, output_path):
            error(f"{table_name}: Export failed")
            report["status"] = "failed"
            break

        info(f"Export success: {table_name} ({len(table)} rows)")

    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
