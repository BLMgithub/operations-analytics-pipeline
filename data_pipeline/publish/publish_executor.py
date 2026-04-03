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

    This component manages the transition of analytical artifacts from
    the internal assembly zones to the production-facing BI environment.

    Workflow:
        1. Integrity Gate: Verifies that the current run has produced all
        required semantic modules and tables defined in the registry.
        2. Promotion: Moves/copies artifacts into a permanent, read-only
        versioned directory (v{run_id}).
        3. Activation: Performs an atomic update of the 'latest' pointer
        to switch BI/Reporting traffic to the new version.

    Operational Guarantees:
    - Atomicity: The 'latest' pointer is updated ONLY if all prior
      validation and promotion steps succeed.
    - Immutability: Promoted versions are treated as static snapshots.
    - Fail-Fast: Any failure in the lifecycle prevents version activation.

    Failure Behavior:
    - Explicit Fail-Fast: Uses 'fail_step' helper to terminate the lifecycle and
      mark status as 'failed' immediately after any step failure.

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
