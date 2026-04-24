# =============================================================================
# Publish Stage Logic
# =============================================================================

import polars as pl
from datetime import datetime as dt, timezone
from google.cloud import bigquery
from contextlib import suppress
from pathlib import Path
import json
import os

from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.semantic.semantic_executor import SEMANTIC_MODULES
from data_pipeline.shared.storage_adapter import (
    upload_publish_artifacts,
    _split_gcs_path,
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
    Enforces the pre-publication structural completeness contract.

    Contract:
    - Structural Validation: Scans the runtime semantic directory and verifies 1:1 parity with SEMANTIC_MODULES registry.
    - Schema Enforcement: Validates that all physical Parquet files contain the required column set.

    Invariants:
    - Completeness: Halts publication if any expected module or table is missing from the file system.
    - Version Alignment: Ensures all files follow the current run_id timestamp convention.

    Outputs:
    - Dict: Report containing 'status' and detailed findings.

    Failures:
    - [Structural] Returns status='failed' if directories are missing, modules mismatch, or schemas are incomplete.
    """

    report = init_report()
    semantic_path = run_context.semantic_path

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]
    day = run_context.run_id[6:8]

    # Validate semantic directory
    if not semantic_path.exists():
        log_error("Semantic directory is missing", report)
        report["status"] = "failed"

        return report

    # Validate semantic module
    expected_modules = set(SEMANTIC_MODULES.keys())
    actual_modules = {
        directory.name for directory in semantic_path.iterdir() if directory.is_dir()
    }

    if actual_modules != expected_modules:
        log_error("Semantic module mismatch", report)
        report["status"] = "failed"

        return report

    # Validate expected semantic file set exactly matches required set
    for module_name, module in SEMANTIC_MODULES.items():
        module_path = semantic_path / module_name

        expected_files = {
            f"{table_name}_{year}_{month}_{day}.parquet"
            for table_name in module["tables"]
        }

        actual_files = {file.name for file in module_path.glob("*.parquet")}

        if actual_files != expected_files:
            log_error(f"Semantic file set mismatch on {module_name}", report)
            report["status"] = "failed"

            return report

        # Validate required parquet files exist
        for table_name, meta in module["tables"].items():

            file_name = f"{table_name}_{year}_{month}_{day}.parquet"
            file_path = module_path / file_name

            # Validate required schema columns present
            schema = pl.scan_parquet(file_path).collect_schema()
            missing = set(meta["schema"]) - set(schema.keys())

            if missing:
                log_error(f"{file_name} required column(s): {sorted(missing)}", report)
                report["status"] = "failed"

                return report

    return report


# ------------------------------------------------------------
# PROMOTE VALIDATED SEMANTIC
# ------------------------------------------------------------


def promote_semantic_version(run_context: RunContext) -> Dict:
    """
    Manages the archival of the current run into the publication zone.

    Contract:
    - Promote: Transfers validated semantic artifacts from the runtime zone to a permanent versioned destination.
    - Versioning: Creates a new directory following the 'v{run_id}' physical convention.

    Invariants:
    - Immutability: Once promoted, artifacts are treated as static, read-only snapshots.
    - Path Integrity: Destination is derived strictly from run_context.published_path.

    Outputs:
    - Dict: Report logging the promotion status and any transfer errors.

    Failures:
    - [Operational] Returns status='failed' if the version directory already exists or upload fails.
    """

    report = init_report()

    version_path = Path(run_context.version_path)

    if version_path.exists():
        report["status"] = "failed"
        log_error("Version directory already exists", report)

        return report

    try:
        upload_publish_artifacts(run_context)

    except Exception as e:
        report["status"] = "failed"
        log_error(str(e), report)

        return report

    return report


# ------------------------------------------------------------
# PUBLISHED SQL VIEW
# ------------------------------------------------------------


def swap_bigquery_view(run_context: RunContext, location: str | None = None) -> Dict:
    """
    Atomically updates BigQuery External Tables and Views to point to the new version.

    Contract:
    - Versioned Tables: Creates unique external tables for each semantic table in the current run.
    - Stable Views: Replaces existing 'published_' views to point to the new versioned tables.

    Invariants:
    - Multi-System Sync: BI tools connected to views see the new data immediately after DDL success.
    - Cloud Only: Skips SQL updates if the pipeline is running in a local-only environment.

    Outputs:
    - Dict: Report logging the SQL activation status for each module.
    """

    report = init_report()
    latest_path = run_context.latest_pointer_path
    published_uri = run_context.storage_published_path

    if not str(latest_path).startswith("gs://"):
        log_info("Skipping BigQuery swap (Local Storage detected)", report)

        return report

    # Use provided location or fallback to environment variable (set by Terraform)
    effective_location = location or os.getenv("GCP_REGION", "us-east1")

    try:

        client = bigquery.Client(location=effective_location)
        run_id = run_context.run_id
        project = client.project

        for module_name, module_config in SEMANTIC_MODULES.items():
            for table_name in module_config["tables"]:

                # Create Versioned External Table
                table_ddl = f"""
                CREATE OR REPLACE EXTERNAL TABLE `{project}.{module_name}.{table_name}_v{run_id}`
                OPTIONS (
                    format = 'PARQUET',
                    uris = ['{published_uri}/v{run_id}/{module_name}/{table_name}_*.parquet']
                )
                """

                # Atomic Pointer Swap (View)
                view_ddl = f"""
                CREATE OR REPLACE VIEW `{project}.{module_name}.published_{table_name}` AS
                SELECT * FROM `{project}.{module_name}.{table_name}_v{run_id}`
                """

                client.query(table_ddl, location=effective_location).result()
                client.query(view_ddl, location=effective_location).result()

            log_info(f"BigQuery swap successful for module: {module_name}", report)

    except Exception as e:
        report["status"] = "failed"
        log_error(f"BigQuery Swap Failed: {e}", report)

    return report


# ------------------------------------------------------------
# PUBLISHED ATOMIC POINTER
# ------------------------------------------------------------


def activate_published_version(run_context: RunContext) -> Dict:
    """
    Atomically updates the system-wide 'latest' version pointer.

    Contract:
    - Atomic Update: Overwrites the root 'latest_version.json' to shift downstream consumers to the new run.
    - BI Consistency: Guarantees that analytical tools see the new version only after successful promotion.

    Invariants:
    - Pointer Integrity: Manifest always contains current run_id and ISO-8601 publication timestamps.
    - Atomicity: Local updates use a write-and-replace (os.replace) strategy to prevent manifest corruption.

    Outputs:
    - Dict: Report logging the activation status.

    Failures:
    - [Operational] Returns status='failed' if manifest generation or storage upload (Local/GCS) fails.
    """

    report = init_report()

    latest_path = run_context.latest_pointer_path
    run_dt = dt.strptime(run_context.run_id[:15], "%Y%m%dT%H%M%S")

    payload = {
        "run_id": run_context.run_id,
        "version": f"v{run_context.run_id}",
        "run_year": run_dt.year,
        "run_month": run_dt.month,
        "run_week_of_month": (run_dt.day - 1) // 7 + 1,
        "published_at": dt.now(timezone.utc).isoformat(),
    }

    # LOCAL storage
    if not str(latest_path).startswith("gs://"):

        latest_path = Path(latest_path)
        latest_path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = latest_path.with_suffix(".tmp")

        try:
            with open(tmp_path, "w") as file:
                json.dump(payload, file, indent=2)

            os.replace(tmp_path, latest_path)

        except Exception as e:
            report["status"] = "failed"
            log_error(str(e), report)

            # Cleanup  tmp_path
            with suppress(Exception):
                tmp_path.unlink()

    # GCS storage
    else:
        from google.cloud import storage

        try:
            client = storage.Client()

            bucket_name, prefix = _split_gcs_path(str(latest_path))

            bucket = client.bucket(bucket_name)

            blob = bucket.blob(prefix)

            blob.upload_from_string(
                json.dumps(payload, indent=2),
                content_type="application/json",
            )

        except Exception as e:
            report["status"] = "failed"
            log_error(str(e), report)

    return report
