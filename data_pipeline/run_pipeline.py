# =============================================================================
# PIPELINE EXECUTOR
# =============================================================================

from pathlib import Path
from shutil import copytree
from datetime import datetime as dt
import sys
import json


from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.validate_raw_data import apply_validation
from data_pipeline.stages.apply_raw_data_contract import apply_contract
from data_pipeline.stages.assemble_validated_events import assemble_events
from data_pipeline.stages.build_bi_semantic_layer import build_semantic_layer
from data_pipeline.stages.publish_lifecycle import execute_publish_lifecycle


# ------------------------------------------------------------
# SUPPORTING UTILITIES
# ------------------------------------------------------------


def snapshot_raw_storage(run_context: RunContext) -> None:
    """
    Creates a run-scoped raw snapshot by copying the entire source raw
    directory into the run context.
    """

    source = run_context.storage_raw_path
    destination = run_context.raw_snapshot_path

    if not source.exists():
        raise FileNotFoundError(f"Raw source path not found: {source}")

    copytree(source, destination, dirs_exist_ok=True)


def persist_json(path: Path, payload: dict) -> None:
    """
    Writes the stage report to a JSON file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def initiliaze_metadata(run_context: RunContext) -> None:
    """
    Run metadata initializer.

    Creates the run-scoped metadata record at pipeline start to
    establish lifecycle tracking and publish eligibility state.
    """

    run_dt = dt.strptime(run_context.run_id[:15], "%Y%m%dT%H%M%S")

    payload = {
        "run_id": run_context.run_id,
        "status": "RUNNING",
        "started_at": dt.utcnow().isoformat(),
        "run_year": run_dt.year,
        "run_month": run_dt.month,
        "run_week_of_month": (run_dt.day - 1) // 7 + 1,
        "completed_at": None,
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

    payload["status"] = status
    payload["completed_at"] = dt.utcnow().isoformat()

    if status == "SUCCESS":
        payload["published"] = True

    else:
        payload["published"] = False

    persist_json(run_context.metadata_path, payload)


# ------------------------------------------------------------
# PIPELINE ORCHESTRATOR
# ------------------------------------------------------------


def main() -> None:
    """
    Pipeline orchestrator.

    Execution order:
    1. Snapshot raw data
    2. Initialize metadata (RUNNING)
    3. Validation → halt on errors
    4. Contract enforcement
        - Apply role-driven repair
        - Propagate invalid order_ids (parent → child)
    5. Re-validation → halt on errors/warnings
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
    run_context.initialize_directories()

    # Create raw snapshot at runtime
    snapshot_raw_storage(run_context)
    initiliaze_metadata(run_context)

    # Initial validation
    validation_initial = apply_validation(run_context)

    persist_json(
        run_context.logs_path / "validation_initial.json",
        {
            "run_id": run_context.run_id,
            "report": validation_initial,
        },
    )

    # Early exit for structural errors else apply contract
    if validation_initial["errors"]:
        finalize_metadata(run_context, "FAILED")
        sys.exit(1)

    report_contract = []

    # Accumulates invalid order_ids produced by parent (event_fact) tables and
    # applies them to child (transaction_detail) tables during the same run for cascading.
    invalid_order_ids = set()

    # TABLE_CONFIG order must list parent tables before their children.
    for table_name in TABLE_CONFIG:

        contract, new_invalid_ids = apply_contract(
            run_context,
            table_name,
            invalid_order_ids,
        )

        invalid_order_ids |= new_invalid_ids
        report_contract.append(contract)

    persist_json(
        run_context.logs_path / "contract_report.json",
        {
            "run_id": run_context.run_id,
            "report": report_contract,
        },
    )

    # Rerun validation on CONTRACTED data
    validation_post_contract = apply_validation(
        run_context,
        base_path=run_context.contracted_path,
    )

    persist_json(
        run_context.logs_path / "validation_post_contract.json",
        {
            "run_id": run_context.run_id,
            "report": validation_post_contract,
        },
    )

    # Intervention: Either manual fixing or escalate the data to source owner
    if validation_post_contract["errors"] or validation_post_contract["warnings"]:
        finalize_metadata(run_context, "FAILED")
        sys.exit(1)

    # Assemble event table
    assemble = assemble_events(run_context)

    persist_json(
        run_context.logs_path / "assemble_report.json",
        {
            "run_id": run_context.run_id,
            "report": assemble,
        },
    )

    if assemble["status"] == "failed":
        finalize_metadata(run_context, "FAILED")
        sys.exit(1)

    # Semantic modeling
    semantic = build_semantic_layer(run_context)

    persist_json(
        run_context.logs_path / "semantic_report.json",
        {
            "run_id": run_context.run_id,
            "report": semantic,
        },
    )

    if semantic["status"] == "failed":
        finalize_metadata(run_context, "FAILED")
        sys.exit(1)

    # Pre-publish semantic validation
    publish = execute_publish_lifecycle(run_context)

    persist_json(
        run_context.logs_path / "publish_report.json",
        {
            "run_id": run_context.run_id,
            "report": publish,
        },
    )

    if publish["status"] == "failed":
        finalize_metadata(run_context, "FAILED")
        sys.exit(1)

    finalize_metadata(run_context, "SUCCESS")
    sys.exit(0)


if __name__ == "__main__":
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================
