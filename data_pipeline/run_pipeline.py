# =============================================================================
# Pipeline Orchestrator
# =============================================================================


from pathlib import Path
from datetime import datetime as dt, timezone
import json
import os
import shutil

from google.cloud import bigquery
from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from data_pipeline.validation.validation_executor import apply_validation
from data_pipeline.contract.contract_executor import apply_contract
from data_pipeline.assembly.assembly_executor import assemble_events, force_gc
from data_pipeline.semantic.semantic_executor import build_semantic_layer
from data_pipeline.publish.publish_executor import execute_publish_lifecycle

from data_pipeline.shared.storage_adapter import (
    download_raw_snapshot,
    upload_run_artifacts,
    upload_contracted_directory,
    # download_contracted_datasets,
)

import psutil
import threading
import time


def memory_logger(stop_event: threading.Event):
    """Temporary: Logs RAM usage to stdout every 1s for benchmarking."""
    while not stop_event.is_set():
        mem_mb = psutil.virtual_memory().used / (1024 * 1024)
        print(f"METRIC_MEM: {mem_mb:.2f} MB")
        time.sleep(1)


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
        "pipeline_version": "v5.1",
        "status": "RUNNING",
        "started_at": dt.now(timezone.utc).isoformat(),
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
    completion_time = dt.now(timezone.utc)

    payload["status"] = status
    payload["completed_at"] = completion_time.isoformat()

    duration = completion_time - start_time
    mm, ss = divmod(int(duration.total_seconds()), 60)

    payload["run_duration"] = f"{mm:02d}m {ss:02d}s"
    payload["published"] = status == "SUCCESS"

    persist_json(run_context.metadata_path, payload)


def refresh_bq_external_cache(run_context: RunContext) -> None:
    """
    Forces BigQuery to refresh the metadata cache for a BigLake/External table.

    Contract:
    - Connectivity: Initializes a BigQuery client using Application Default Credentials.
    - Execution: Invokes the BQ.REFRESH_EXTERNAL_METADATA_CACHE system procedure.
    - Idempotency: Can be safely called multiple times without data mutation.

    Invariants:
    - State Sync: Ensures downstream stages (like Assembly via Storage Read API) see the
      newly contracted Parquet files immediately, bypassing BigQuery's default metadata caching delay.
    """
    project_id = run_context.bq_project_id
    dataset_id = run_context.bq_dataset_id
    location = os.getenv("GCP_REGION", "MISSING_REGION")

    if location == "MISSING_REGION":
        print("[INFO] Skipping BigQuery cache refresh (Local Storage detected)")

        return

    try:
        for table_name in TABLE_CONFIG:
            client = bigquery.Client(project=project_id, location=location)
            table_path = f"{project_id}.{dataset_id}.{table_name}"
            query = f"CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('{table_path}')"

            # Execute the query synchronously
            client.query(query).result()

    except Exception as e:
        print(f"Failed to refresh BigQuery cache for {dataset_id}: {e}")

    print(f"Successfully refreshed BigQuery cache for {dataset_id}")


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
    Ultimate authority for the end-to-end data pipeline lifecycle coordination.

    Workflow:
    1. Resolve: Instantiate RunContext and initialize background memory telemetry.
    2. Hydrate (Raw): Synchronize the raw data snapshot from Cloud Storage to local workspace.
    3. Initialize: Register the run commencement and capture initial metadata.
    4. Validate (Raw): Assert raw data sanity and fail-fast on structural errors.
    5. Contract: Execute subtractive filtering and schema freezing for Silver-layer datasets.
    6. Revalidate: Defensive check to ensure contracted artifacts meet downstream requirements.
    7. Promote (Silver): Persist delta contracted datasets to Cloud Silver Storage.
    8. Synchronize (BQ): Force refresh of BigQuery external metadata cache for immediate visibility.
    9. Purge (Local): Reclaim local disk and RAM by evicting raw/contracted sources before Assembly.
    10. Assemble: Flatten relational data into a unified Gold event layer using Storage Read API.
    11. Model (Semantic): Build entity-centric analytical modules (Fact/Dim).
    12. Publish: Execute final lifecycle validation, BigQuery view swap, and atomic pointer swap for the 'latest' version.
    13. Finalize: Update terminal metadata, upload all stage reports/telemetry, and purge workspace.

    Operational Guarantees:
    - Defensive Integrity: Prevents promotion of invalid data to Silver/Gold layers via multi-gate validation.
    - Memory Efficiency: Enforces deterministic 'Purge' and 'GC' cycles between heavy processing stages.
    - Traceability: Maintains strict run_id consistency across local and Cloud artifact lineages.
    - Resilience: Guarantees telemetry upload (logs/metadata) even during catastrophic stage failures.

    Side Effects:
    - Writes local stage reports and metadata to the run-specific workspace.
    - Mutates Cloud Storage (Silver layer artifacts, Run Artifacts).
    - Refreshes BigQuery system procedures (Metadata Cache).
    - Swaps production environment pointers during the Publish stage.

    Failure Behavior:
    - Crash: Any stage RuntimeError triggers a 'FAILED' status update and immediate local cleanup.
    - Recovery: Logs are persisted to Cloud before exit to enable post-mortem analysis.
    """

    run_context = RunContext.create()

    stop_event = threading.Event()
    logger_thread = threading.Thread(target=memory_logger, args=(stop_event,))
    logger_thread.start()

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

        # Refresh BQ Metadata Cache after uploading is complete
        refresh_bq_external_cache(run_context)

        # Clear RAM memory from previous stages
        if os.path.exists(run_context.raw_snapshot_path):
            shutil.rmtree(run_context.raw_snapshot_path)
            shutil.rmtree(run_context.contracted_path)
        force_gc()

        run_assemble_events_stage(run_context)
        force_gc()

        run_semantic_modeling_stage(run_context)
        force_gc()

        run_prepublishing_validation_stage(run_context)

        finalize_metadata(run_context, status="SUCCESS")

    except Exception:

        finalize_metadata(run_context, status="FAILED")
        raise

    finally:
        stop_event.set()
        logger_thread.join()

        # Persist run artifacts (logs/metadata) Pass or Fail
        upload_run_artifacts(run_context)

        # Clean RAM memory for next run
        if os.path.exists(run_context.workspace_root):
            shutil.rmtree(run_context.workspace_root)
        force_gc()


if __name__ == "__main__":
    main()


# =============================================================================
# END OF SCRIPT
# =============================================================================
