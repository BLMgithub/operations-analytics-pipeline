# =============================================================================
# Publish Stage Executor
# =============================================================================

from data_pipeline.shared.run_context import RunContext
from typing import Dict
from data_pipeline.publish.publish_logic import (
    run_integrity_gate,
    promote_semantic_version,
    activate_published_version,
    log_info,
)


def execute_publish_lifecycle(run_context: RunContext) -> Dict:
    """
    Main entry point for the Pipeline Publish Stage.

    Workflow:
    1. Validate: Executes the 'Integrity Gate' to ensure all semantic artifacts exist and are schema-compliant.
    2. Promote: Transfers validated artifacts to the permanent versioned publication zone.
    3. Delegate: Triggers the atomic pointer swap to activate the new version for BI consumers.

    Operational Guarantees:
    - Atomicity: The 'latest' version pointer is updated ONLY after successful promotion of all artifacts.
    - Immutability: Once published, a versioned directory is treated as a static, read-only snapshot.
    - Fail-Fast: Any failure in validation or promotion immediately halts the lifecycle.

    Side Effects:
    - Persists a new versioned directory (v{run_id}) in the publication zone.
    - Mutates the 'latest_version.json' manifest to update the global version pointer.

    Failure Behavior:
    - Traps step-level failures; logs errors and returns a report with status='failed', preventing version activation.

    Returns:
        Dict: A global publish report containing status and step-level logs.
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
