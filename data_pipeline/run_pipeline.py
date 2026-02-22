# =============================================================================
# PIPELINE EXECUTOR
# =============================================================================

from pathlib import Path
from shutil import copytree
import sys
import json

from data_pipeline.shared.table_configs import TABLE_CONFIG
from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.validate_raw_data import apply_validation
from data_pipeline.stages.apply_raw_data_contract import apply_contract
from data_pipeline.stages.assemble_validated_events import assemble_events


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


def main() -> None:
    run_context = RunContext.create()
    run_context.initialize_directories()

    # Create raw snapshot at runtime
    snapshot_raw(run_context)

    report_validation_1 = []

    # Initial validation
    validation_1 = apply_validation(run_context)
    report_validation_1.append(validation_1)

    persist_json(
        run_context.logs_path / "validation_1.json",
        {
            "run_id": run_context.run_id,
            "report": report_validation_1,
        },
    )

    # Early exit for structural errors else apply contract
    if validation_1["errors"]:
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

    report_validation_2 = []

    # Rerun validation on CONTRACTED data
    validation_2 = apply_validation(
        run_context,
        base_path=run_context.contracted_path,
    )

    report_validation_2.append(validation_2)

    persist_json(
        run_context.logs_path / "validation_2.json",
        {
            "run_id": run_context.run_id,
            "report": report_validation_2,
        },
    )

    # Intervention: Either manual fixing or escalate the data to source owner
    if validation_2["errors"] or validation_2["warnings"]:
        sys.exit(1)

    report_assemble = []

    # Assemble event table
    assemble = assemble_events(run_context)
    report_assemble.append(assemble)

    persist_json(
        run_context.logs_path / "assemble_report.json",
        {
            "run_id": run_context.run_id,
            "report": report_assemble,
        },
    )

    if assemble["status"] == "failed":
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================
