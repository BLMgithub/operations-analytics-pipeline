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
    Execute full publish lifecycle for semantic artifacts.

    Execution steps:
    - Run semantic integrity gate
    - Promote validated artifacts into version directory
    - Atomically activate published pointer

    Guarantees:
    - Only fully validated semantic versions become visible to BI.

    Failure behavior:
    - Any step failure halts publish lifecycle and prevents activation.
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
