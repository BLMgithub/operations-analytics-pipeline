# =============================================================================
# PUBLISH ACTIVATION GATE
# =============================================================================

import pandas as pd
import shutil
from datetime import datetime as dt
import json
import os

from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.shared.table_configs import (
    SELLER_FACT_ENFORCED_SCHEMA,
    SELLER_DIM_ENFORCED_SCHEMA,
)

# ------------------------------------------------------------
# ASSEMBLE REPORT & LOGS
# ------------------------------------------------------------


def init_report():
    return {"status": "success", "errors": [], "info": []}


def log_info(message: str, report: Dict[str, List[str]]) -> None:
    print(f"[INFO] {message}")
    report["info"].append(message)


def log_error(message: str, report: Dict[str, list[str]]) -> None:
    print(f"[ERROR] {message}")
    report["errors"].append(message)


# ------------------------------------------------------------
# PRE-PUBLISH INTEGRITY GATE
# ------------------------------------------------------------


def run_integrity_gate(run_context: RunContext) -> Dict:
    """
    Pre-publish semantic integrity gate.

    Verifies that the semantic layer is complete, structurally valid,
    and safe for downstream consumption before any publish action.

    Chronological behavior:

    - Initializes run-scoped reporting.
    - Validates semantic output directory exists.
    - Confirms actual parquet file set exactly matches the expected set.
    - Loads each required semantic table.
    - Validates each table is readable and non-empty.
    - Verifies required schema columns are present per table type.
    - Emits success signal when all checks pass.

    Gate intent:

    - Detect partial publishes
    - Detect schema drift entering BI layer
    - Detect empty or corrupt semantic outputs
    """

    report = init_report()
    semantic_path = run_context.semantic_path

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]

    # Validate semantic directory exists
    if not semantic_path.exists():
        log_error("Semantic directory is missing", report)
        report["status"] = "failed"

        return report

    # Validate expected semantic file set exactly matches required set
    seller_expected_files = {
        f"seller_week_performance_fact_{year}_{month}.parquet",
        f"seller_dim_{year}_{month}.parquet",
    }

    seller_actual_files = {
        file.name for file in run_context.semantic_path.glob("*.parquet")
    }

    if seller_actual_files != seller_expected_files:
        log_error("Semantic file set mismatch", report)
        report["status"] = "failed"

        return report

    # Validate required parquet files exist
    for file_name in seller_expected_files:
        path = semantic_path / file_name

        try:
            df = pd.read_parquet(path)

        except Exception as e:
            log_error(f"{file_name} failed to load: {e}", report)
            report["status"] = "failed"

            return report

        # Validate dataframe not empty
        if df is None or df.empty:
            log_error(f"{file_name} logical table missing or empty", report)
            report["status"] = "failed"

            return report

        # Validate required schema columns present
        if "seller_week_performance_fact" in file_name:
            required_cols = SELLER_FACT_ENFORCED_SCHEMA
        else:
            required_cols = SELLER_DIM_ENFORCED_SCHEMA

        missing = set(required_cols) - set(df.columns)

        if missing:
            log_error(f"{file_name} required column(s): {sorted(missing)}", report)
            report["status"] = "failed"

            return report

    log_info("Pre-publishing validation passed", report)
    return report


# ------------------------------------------------------------
# PROMOTE VALIDATED SEMANTIC
# ------------------------------------------------------------


def promote_semantic_version(run_context: RunContext) -> Dict:
    """
    Semantic version promotion step.

    Publishes the validated semantic artifacts into the run-scoped
    version directory for downstream consumption and lineage tracking.

    Chronological behavior:

    - Initializes run-scoped reporting.
    - Verifies the target version directory does not already exist.
    - Creates the version directory for the current run.
    - Copies all semantic parquet artifacts into the version directory.
    - Emits success signal when promotion completes.

    Promotion intent:

    - Create an immutable, run-versioned semantic snapshot
    - Provide a stable handoff point for BI consumption
    - Preserve lineage between run_id and published artifacts
    """

    report = init_report()

    semantic_path = run_context.semantic_path
    version_path = run_context.version_path / "seller_semantic"

    if version_path.exists():
        report["status"] = "failed"
        log_error("Version directory already exists", report)

        return report

    # Create version directory
    try:
        version_path.mkdir(parents=True, exist_ok=False)

    except Exception as e:
        report["status"] = "failed"
        log_error(str(e), report)

        return report

    # Copy validated semantics to version directory
    try:
        for file in semantic_path.glob("*.parquet"):
            shutil.copy2(file, version_path / file.name)

    except Exception as e:
        report["status"] = "failed"
        log_error(str(e), report)

        return report

    log_info("Semantic artifacts promoted successfully", report)

    return report


# ------------------------------------------------------------
# PUBLISHED ATOMIC POINTER
# ------------------------------------------------------------


def activate_published_version(run_context: RunContext) -> Dict:
    """
    Published version activation step.

    Atomically updates the latest-version pointer to the newly promotedsemantic snapshot. <br>
    Guarantee BI dashboards read only fully published versions.

    Chronological behavior:

    - Initializes run-scoped reporting.
    - Builds the pointer payload with run lineage metadata.
    - Writes payload to a temporary pointer file.
    - Atomically swaps the temporary file into the latest pointer path.
    - Emits success signal when the swap completes.

    Notes:
    - Uses temp-file + os.replace for atomicity.
    - Assumes version promotion has already succeeded.
    """

    report = init_report()

    latest_path = run_context.latest_pointer_path
    tmp_path = latest_path.with_suffix(".tmp")

    payload = {
        "run_id": run_context.run_id,
        "version": f"v{run_context.run_id}",
        "published_at": dt.utcnow().isoformat(),
    }

    try:
        with open(tmp_path, "w") as file:
            json.dump(payload, file, indent=2)

        os.replace(tmp_path, latest_path)

    except Exception as e:
        report["status"] = "failed"
        log_error(str(e), report)

    log_info("Atomic pointer swap successful", report)

    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
