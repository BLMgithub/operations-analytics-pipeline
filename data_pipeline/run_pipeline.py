# =============================================================================
# Pipeline Orchestrator
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
    Serializes state dictionaries to the local filesystem.

    Contract:
    - Creates parent directories if they do not exist.
    - Performs standard JSON serialization with 2-space indentation.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def stage_logger(run_context: RunContext, stage: str, report: dict | list):
    """
    Standardizes the persistence of stage-level telemetry.

    Contract:
    - Maps 'stage' identifiers to deterministic filenames (e.g., stage_report.json).
    - Persists reports to the 'run_context.run_path'.
    """

    persist_json(
        path=run_context.logs_path / f"{stage}.json",
        payload={"run_id": run_context.run_id, "report": report},
    )


def initialize_metadata(run_context: RunContext) -> None:
    """
    Registers the commencement of a pipeline run.

    Contract:
    - Generates a 'run_metadata.json' artifact.
    - Captures the initial 'run_id', 'start_time', and sets status to 'RUNNING'.
    """

    run_dt = dt.strptime(run_context.run_id[:15], "%Y%m%dT%H%M%S")

    payload = {
        "run_id": run_context.run_id,
        "pipeline_version": "v5",
        "status": "RUNNING",
        "started_at": dt.utcnow().isoformat(),
        "run_year": run_dt.year,
        "run_month": run_dt.month,
        "run_day": run_dt.day,
        "completed_at": None,
        "run_duration": None,
        "published": False,
    }

    persist_json(run_context.metadata_path, payload)


def finalize_metadata(run_context: RunContext, status: str) -> None:
    """
    Updates the run metadata record with terminal status and completion timestamp.

    Contract:
    - Updates 'run_metadata.json' with the 'end_time' and final 'status'.
    - Calculates the total run duration and display in '00m 00s' format.
    """

    if not run_context.metadata_path.exists():
        raise RuntimeError("metadata.json missing during finalization")

    with open(run_context.metadata_path, "r") as file:
        payload = json.load(file)

    start_time = dt.fromisoformat(payload["started_at"])
    completion_time = dt.utcnow()

    payload["status"] = status
    payload["completed_at"] = completion_time.isoformat()

    duration = completion_time - start_time
    mm, ss = divmod(int(duration.total_seconds()), 60)

    payload["run_duration"] = f"{mm:02d}m {ss:02d}s"
    payload["published"] = status == "SUCCESS"

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
    Ultimate authority for the end-to-end data pipeline lifecycle.

    Workflow:
        1.  Initialization: Resolve RunContext and instantiate global run metadata.
        2.  Ingestion: Synchronize the raw data snapshot from Cloud Storage to local workspace.
        3.  Gate I (Validation): Assert raw data sanity; fail-fast on fatal structural errors.
        4.  Processing (Contract): Execute subtractive filtering and Silver-layer schema freezing.
        5.  Gate II (Revalidation): Defensive check to ensure 'contracted' data is valid.
        6.  Persistence (Sync Upload): Promote local contracted artifacts to the Cloud Silver Storage.
        7.  Resource Reclamation: Purge transient directories (raw/contracted) to optimize memory.
        8.  Hydration (Sync Download): Restore local environment with the full accumulated Silver state.
        9.  Integration (Assembly): Flatten relational data into a unified Gold event layer.
        10. Modeling (Semantic): Build entity-centric analytical modules (Fact/Dim).
        11. Gate III (Pre-Publish): Final verification of semantic artifact completeness.
        12. Activation (Publish): Atomic swap of the production 'latest' version pointer.
        13. Finalization: Persist all telemetry/logs to Cloud and purge the local workspace.

    Operational Guarantees:
    - Defensive Integrity: No data moves to 'Assembly' without passing 'Revalidation'.
    - Silver Continuity: Uses a Cloud-Sync loop to ensure Assembly operates on the full delta state.
    - Resource Stewardship: Mandatory local cleanup via global 'finally' block to prevent disk leaks.
    - Traceability: Enforces atomic 'run_id' consistency across all 13 lifecycle steps.
    - Visibility: Guarantees cloud-upload of stage reports even in partial failure scenarios.
    """

    run_context = RunContext.create()

    # Pre-start cleaning
    if os.path.exists(run_context.workspace_root):
        shutil.rmtree(run_context.workspace_root, ignore_errors=True)

    try:

        download_raw_snapshot(run_context)
        initialize_metadata(run_context)

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
