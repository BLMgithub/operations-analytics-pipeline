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
from data_pipeline.stages.publish_lifecycle import (
    run_integrity_gate,
    promote_semantic_version,
    activate_published_version,
)


# ------------------------------------------------------------
# SUPPORTING UTILITIES
# ------------------------------------------------------------


def snapshot_raw(run_context: RunContext) -> None:
    """
    Creates a run-scoped raw snapshot by copying the entire source raw
    directory into the run context.
    """

    source = run_context.source_raw_path
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

    payload = {
        "run_id": run_context.run_id,
        "status": "RUNNING",
        "started_at": dt.utcnow().isoformat(),
        "completed_at": None,
        "published": False,
    }

    persist_json(run_context.metadata_path, payload)


def finalize_run(run_context: RunContext, status: str) -> None:
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
    Pipeline execution controller.

    Execution order:

    1. Initialize run context and directory structure.
    2. Capture raw snapshot and initialize metadata.
    3. Run initial validation on raw data.
       - Exit if structural errors exist.
    4. Apply table contracts in configured parent → child order,
       propagating invalid order_ids.
    5. Rerun validation on contracted data.
       - Exit if any errors or warnings remain.
    6. Assemble the core event table.
       - Exit on assembly failure.
    7. Build semantic layer tables.
       - Exit on semantic failure.
    8. Run pre-publish semantic integrity gate.
       - Exit if gate fails.
    9. Exit process with success code.
    """

    run_context = RunContext.create()
    run_context.initialize_directories()

    # Create raw snapshot at runtime
    snapshot_raw(run_context)
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
        finalize_run(run_context, "FAILED")
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
        finalize_run(run_context, "FAILED")
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
        finalize_run(run_context, "FAILED")
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
        finalize_run(run_context, "FAILED")
        sys.exit(1)

    # Pre-publish semantic integrity validation
    gate = run_integrity_gate(run_context)

    persist_json(
        run_context.logs_path / "publish_integrity_report.json",
        {
            "run_id": run_context.run_id,
            "report": gate,
        },
    )

    if gate["status"] == "failed":
        finalize_run(run_context, "FAILED")
        sys.exit(1)

    # Copy validated semantics to version directory
    promotion = promote_semantic_version(run_context)

    persist_json(
        run_context.logs_path / "publish_promotion_report.json",
        {
            "run_id": run_context.run_id,
            "report": promotion,
        },
    )

    if promotion["status"] == "failed":
        finalize_run(run_context, "FAILED")
        sys.exit(1)

    finalize_run(run_context, "SUCCESS")

    activation = activate_published_version(run_context)

    persist_json(
        run_context.logs_path / "publish_activation_report.json",
        {
            "run_id": run_context.run_id,
            "report": activation,
        },
    )

    if activation["status"] == "failed":
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================
