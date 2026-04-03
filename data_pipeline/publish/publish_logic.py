# =============================================================================
# Publish Stage Logic
# =============================================================================

import polars as pl
from datetime import datetime as dt
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
    - Scans the runtime semantic directory for existence.
    - Validates that every Module and Table defined in SEMANTIC_MODULES
      exists as a physical artifact.

    Invariants:
    - Failure is triggered if any expected Parquet file is missing.

    Returns:
        Dict: A report object containing the success status and findings.
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
    - Creates a permanent directory following the 'v{run_id}' convention.
    - Transfers all semantic artifacts to the versioned destination.

    Invariants:
    - Destination is derived from run_context.published_path.
    - Relies on the storage_adapter for Local/GCS transparency.

    Returns:
        Dict: A report object logging the promotion status.
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
# PUBLISHED ATOMIC POINTER
# ------------------------------------------------------------


def activate_published_version(run_context: RunContext) -> Dict:
    """
    Atomically updates the system-wide 'latest' version pointer.

    Contract:
    - Generates a JSON manifest containing run_id and publication metadata.
    - Overwrites the root 'latest_version.json' in the published zone.

    Invariants:
    - Atomic Update: Local updates use write-and-replace to prevent corruption.
    - BI Consistency: Downstream consumers see the new version only after
      this atomic swap is complete.

    Returns:
        Dict: A report object logging the activation status.
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
        "published_at": dt.utcnow().isoformat(),
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
