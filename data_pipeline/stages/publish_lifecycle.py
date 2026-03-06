# =============================================================================
# PUBLISH ACTIVATION GATE
# =============================================================================

import pandas as pd
import shutil
from datetime import datetime as dt
from contextlib import suppress
import json
import os

from typing import Dict, List
from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.build_bi_semantic_layer import SEMANTIC_MODULES

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

    Validations:
    - Semantic directory exists
    - Expected module set matches semantic module registry
    - Expected file set matches semantic registry
    - Parquet files load successfully
    - Tables are non-empty
    - Required schema columns present

    Intent:
    - Prevent partial or corrupt publish
    - Block schema drift into BI layer

    Failure:
    - Returns failed status; promotion and activation are skipped
    """

    report = init_report()
    semantic_path = run_context.semantic_path

    year = run_context.run_id[:4]
    month = run_context.run_id[4:6]

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
            f"{table_name}_{year}_{month}.parquet" for table_name in module["tables"]
        }

        actual_files = {file.name for file in module_path.glob("*.parquet")}

        if actual_files != expected_files:
            log_error(f"Semantic file set mismatch on {module_name}", report)
            report["status"] = "failed"

            return report

        # Validate required parquet files exist
        for table_name, meta in module["tables"].items():

            file_name = f"{table_name}_{year}_{month}.parquet"
            file_path = module_path / file_name

            try:
                df = pd.read_parquet(file_path)

            except Exception as e:
                log_error(str(e), report)
                report["status"] = "failed"

                continue

            if df is None or df.empty:
                log_error(f"{file_name} logical table missing or empty", report)
                report["status"] = "failed"

                return report

            # Validate required schema columns present
            missing = set(meta["schema"]) - set(df.columns)

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
    Promote validated semantic artifacts into immutable version folder.

    Execution:
    - Verify version directory does not already exist
    - Create run-scoped version directory
    - Copy semantic artifacts into version folder

    Intent:
    - Create immutable run-versioned snapshot
    - Preserve lineage between run_id and published artifacts
    - Prepare artifacts for atomic activation

    Failure:
    - Returns failed status; no pointer update occurs
    """

    report = init_report()

    semantic_path = run_context.semantic_path
    version_path = run_context.version_path

    if version_path.exists():
        report["status"] = "failed"
        log_error("Version directory already exists", report)

        return report

    # Create version directory
    try:
        version_path.mkdir(parents=True, exist_ok=False)

        for module_name in SEMANTIC_MODULES:
            source_module_path = semantic_path / module_name
            target_module_path = version_path / module_name

            # Copy validated semantics to version directory
            shutil.copytree(source_module_path, target_module_path)

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
    Atomically activate promoted version.

    Execution:
    - Write version payload to temporary file
    - Replace _latest.json using os.replace (atomic swap)

    Guarantees:
    - Dashboards resolve only fully promoted versions
    - No partial pointer updates possible

    Assumes:
    - Promotion succeeded
    - Metadata finalized prior to activation
    """

    report = init_report()

    latest_path = run_context.latest_pointer_path
    latest_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = latest_path.with_suffix(".tmp")

    run_dt = dt.strptime(run_context.run_id[:15], "%Y%m%dT%H%M%S")

    payload = {
        "run_id": run_context.run_id,
        "version": f"v{run_context.run_id}",
        "run_year": run_dt.year,
        "run_month": run_dt.month,
        "run_week_of_month": (run_dt.day - 1) // 7 + 1,
        "published_at": dt.utcnow().isoformat(),
    }

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

    return report


# ------------------------------------------------------------
# RUN PUBLISH LIFECYCLE
# ------------------------------------------------------------


def execute_publish_lifecycle(run_context: RunContext) -> Dict:
    """
    docstring.
    """

    report = {
        "status": "success",
        "steps": {},
    }

    def fail_step(step_name):
        report["status"] = "failed"
        report["failed_step"] = step_name

        return report

    validate_semantic = run_integrity_gate(run_context)
    report["steps"]["integrity_gate"] = validate_semantic

    if validate_semantic["status"] == "failed":
        return fail_step("integrity_gate")

    log_info("Pre-publishing validation passed", validate_semantic)

    promote_semantic = promote_semantic_version(run_context)
    report["steps"]["promotion"] = promote_semantic

    if promote_semantic["status"] == "failed":
        return fail_step("promotion")

    log_info("Semantic artifacts promoted successfully", promote_semantic)

    published_activation = activate_published_version(run_context)
    report["steps"]["activation"] = published_activation

    if published_activation["status"] == "failed":
        return fail_step("activation")

    log_info("Atomic pointer swap successful", published_activation)

    return report


# =============================================================================
# END OF SCRIPT
# =============================================================================
