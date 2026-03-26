# =============================================================================
# PIPELINE EXECUTOR
# =============================================================================


from pathlib import Path
from datetime import datetime as dt
import json
import os
import shutil
import gc

from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from data_pipeline.validation.validation_executor import apply_validation
from data_pipeline.contract.contract_executor import apply_contract
from data_pipeline.assembly.assembly_executor import assemble_events
from data_pipeline.semantic.semantic_executor import build_semantic_layer
from data_pipeline.publish.publish_executor import execute_publish_lifecycle

from data_pipeline.shared.storage_adapter import (
    download_raw_snapshot,
    upload_run_artifacts,
    upload_contracted_directory,
    download_contracted_datasets,
)


# ------------------------------------------------------------
# SUPPORTING UTILITIES
# ------------------------------------------------------------


def persist_json(path: Path, payload: dict) -> None:
    """
    Writes the stage report to a JSON file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def stage_logger(run_context: RunContext, stage: str, report: dict | list):

    persist_json(
        path=run_context.logs_path / f"{stage}.json",
        payload={"run_id": run_context.run_id, "report": report},
    )


def initiliaze_metadata(run_context: RunContext) -> None:
    """
    Run metadata initializer.

    Creates the run-scoped metadata record at pipeline start to
    establish lifecycle tracking and publish eligibility state.
    """

    run_dt = dt.strptime(run_context.run_id[:15], "%Y%m%dT%H%M%S")

    payload = {
        "run_id": run_context.run_id,
        "pipeline_version": "v4",
        "status": "RUNNING",
        "started_at": dt.utcnow().isoformat(),
        "run_year": run_dt.year,
        "run_month": run_dt.month,
        "run_week_of_month": (run_dt.day - 1) // 7 + 1,
        "completed_at": None,
        "run_duration": None,
        "published": False,
    }

    persist_json(run_context.metadata_path, payload)


def finalize_metadata(run_context: RunContext, status: str) -> None:
    """
    Run metadata finalizer.

    Updates the run metadata record with terminal status and
    completion timestamp.
    """

    if not run_context.metadata_path.exists():
        raise RuntimeError("metadata.json missing during finalization")

    with open(run_context.metadata_path, "r") as file:
        payload = json.load(file)

    completion_time = dt.utcnow()

    payload["status"] = status
    payload["completed_at"] = completion_time.isoformat()

    start_time = dt.fromisoformat(payload["started_at"])
    duration = completion_time - start_time

    mm, ss = divmod(int(duration.total_seconds()), 60)
    payload["run_duration"] = f"{mm:02d}m {ss:02d}s"

    payload["published"] = True if status == "SUCCESS" else False

    persist_json(run_context.metadata_path, payload)


# ------------------------------------------------------------
# STAGE WRAPPERS
# ------------------------------------------------------------


def run_initial_validation_stage(run_context) -> None:

    report = apply_validation(run_context)
    stage_logger(run_context, stage="initial_validation", report=report)

    if report["errors"]:
        raise RuntimeError("Stage failure: Initial Validation")


def run_contract_application_stage(run_context) -> tuple[set, set]:
    report = []

    # Accumulates set of invalid order_ids and valid order_ids, and apply to child tables.
    invalid_ids = set()
    valid_ids = set()

    # NOTE: TABLE_CONFIG order must list parent first before its children.
    for table_name in TABLE_CONFIG:

        contract, new_inv, new_val = apply_contract(
            run_context, table_name, invalid_ids, valid_ids
        )

        invalid_ids |= new_inv
        if new_val:
            valid_ids = new_val

        report.append(contract)

    stage_logger(run_context, stage="contract_application", report=report)
    return invalid_ids, valid_ids


def run_post_contract_validation_stage(run_context) -> None:

    # Checks contracted datasets directory
    report = apply_validation(run_context, base_path=run_context.contracted_path)
    stage_logger(run_context, stage="post_contract_validation", report=report)

    if report["errors"] or report["warnings"]:
        raise RuntimeError("Stage failure: Post Contract Validation")


def run_assemble_events_stage(run_context) -> None:

    report = assemble_events(run_context)
    stage_logger(run_context, stage="assemble_events", report=report)

    if report["status"] == "failed":
        raise RuntimeError("Stage failure: Assemble Events")


def run_semantic_modeling_stage(run_context) -> None:

    report = build_semantic_layer(run_context)
    stage_logger(run_context, stage="semantic_modeling", report=report)

    if report["status"] == "failed":
        raise RuntimeError("Stage failure: Semantic Modeling")


def run_prepublishing_validation_stage(run_context) -> None:

    report = execute_publish_lifecycle(run_context)
    stage_logger(run_context, stage="prepublishing_validation", report=report)

    if report["status"] == "failed":
        raise RuntimeError("Stage failure: Semantic Publishing")


# ------------------------------------------------------------
# PIPELINE ORCHESTRATOR
# ------------------------------------------------------------


def main() -> None:
    """
    Pipeline orchestrator.

    Execution order:
    1. Snapshot raw data
    2. Initialize metadata (RUNNING)
    3. Validation
    4. Contract enforcement
        - Apply role-driven repair
        - Propagate invalid order_ids (parent → child)
        - Propogate valida order_ids (parent → child)
    5. Re-validation
    6. Assemble event table
    7. Build semantic layer
    8. Pre-publish integrity gate
    9. Promote version
    10. Finalize metadata (SUCCESS)
    11. Atomic activation

    Guarantees:
    - Deterministic forward-only execution
    - Single run isolation
    - Only Contract stage mutates data
    - Activation occurs only after SUCCESS

    Failure behavior:
    - Any stage failure → metadata FAILED → process exits
    """

    run_context = RunContext.create()

    # Pre-start cleaning
    if os.path.exists(run_context.workspace_root):
        shutil.rmtree(run_context.workspace_root, ignore_errors=True)

    try:

        download_raw_snapshot(run_context)
        initiliaze_metadata(run_context)

        run_initial_validation_stage(run_context)
        run_contract_application_stage(run_context)
        run_post_contract_validation_stage(run_context)

        # Persist delta contracted datasets to silver storage
        upload_contracted_directory(run_context)

        # Clear RAM memory from previous stages
        if os.path.exists(run_context.raw_snapshot_path):
            shutil.rmtree(run_context.raw_snapshot_path)
            shutil.rmtree(run_context.contracted_path)
        gc.collect()

        # Recreate path and download contract data from silver storage
        run_context.contracted_path.mkdir(parents=True, exist_ok=True)
        download_contracted_datasets(run_context)

        run_assemble_events_stage(run_context)
        gc.collect()

        run_semantic_modeling_stage(run_context)
        gc.collect()

        run_prepublishing_validation_stage(run_context)

        finalize_metadata(run_context, status="SUCCESS")

    except Exception:

        finalize_metadata(run_context, status="FAILED")
        raise

    finally:
        # Persist run artifacts (logs/metadata) Pass or Fail
        upload_run_artifacts(run_context)

        # Clean RAM memory for next run
        if os.path.exists(run_context.workspace_root):
            shutil.rmtree(run_context.workspace_root)
        gc.collect()


if __name__ == "__main__":
    main()


# =============================================================================
# END OF SCRIPT
# =============================================================================
