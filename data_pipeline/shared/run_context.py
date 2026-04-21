# =============================================================================
# Runtime path and directory context
# =============================================================================

from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from datetime import datetime as dt, timezone
import uuid


def _generate_run_id() -> str:
    timestamp = dt.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    random_suffix = uuid.uuid4().hex[:6]
    return f"{timestamp}_{random_suffix}"


@dataclass
class RunContext:
    """
    Central authority for path resolution and execution isolation.

    Contract:
    - Generates a globally unique 'run_id' using timestamp-hex entropy.
    - Defines a deterministic, run-scoped directory tree in the local workspace.
    - Maps local runtime paths to persistent Cloud Storage prefixes.

    Invariants:
    - Isolation: Every run instance operates in a private, unique folder tree.
    - Determinism: Stage-specific paths (raw, contracted, semantic) are
      derived consistently from the 'run_id'.
    - Immutability: Once instantiated, path mappings do not change.
    """

    run_id: str
    base_path: str | Path

    # Workspace paths
    workspace_root: Path
    workspace_run_path: Path

    raw_snapshot_path: Path
    contracted_path: Path
    assembled_path: Path
    semantic_path: Path
    logs_path: Path
    metadata_path: Path

    # Storage paths
    storage_raw_path: str
    storage_contracted_path: str
    storage_mapping_path: str
    storage_published_path: str
    version_path: str
    latest_pointer_path: str
    storage_runs_path: str

    # BigQuery coordinates
    bq_project_id: str
    bq_dataset_id: str

    # NOTE: base =./runtime and storage= ./data were local test paths
    @classmethod
    def create(
        cls,
        base: str | Path = "/tmp",  # "./runtime",
        storage: str | Path = "gs://ops-pipeline-storage-dev",  # "./data",
        run_id: str | None = None,
        run_id_factory: Callable[[], str] | None = None,
        bq_project_id: str | None = None,
        bq_dataset_id: str | None = None,
    ) -> "RunContext":
        """
        Factory method for instantiating a fresh execution context.

        Inputs:
        - base_path: The local root directory for the pipeline workspace.
        - storage_root: The cloud root (GCS bucket or local persistent path).

        Returns:
            RunContext: An initialized context with all path mappings resolved.
        """

        import os

        base_path = Path(base)

        if run_id is None:
            generator = run_id_factory or _generate_run_id
            run_id = generator()

        # Workspace paths
        workspace_root = base_path / "workspace"
        workspace_run_path = workspace_root / "runs" / run_id

        raw_snapshot_path = workspace_run_path / "raw_snapshot"
        contracted_path = workspace_run_path / "contracted"
        assembled_path = workspace_run_path / "assembled"
        semantic_path = workspace_run_path / "semantic"
        logs_path = workspace_run_path / "logs"
        metadata_path = workspace_run_path / "metadata.json"

        # Storage paths
        storage_root = str(storage)
        storage_raw_path = f"{storage_root}/raw"
        storage_contracted_path = f"{storage_root}/contracted"
        storage_mapping_path = f"{storage_root}/id_mapping"
        storage_published_path = f"{storage_root}/published"
        version_path = f"{storage_published_path}/v{run_id}"
        latest_pointer_path = f"{storage_published_path}/_latest.json"
        storage_runs_path = f"{storage_root}/run_artifact/{run_id}"

        bq_project_id = os.getenv("GCP_PROJECT", "PROJECT_ID_NOT_DETECTED")

        if not bq_project_id:
            bq_project_id = "PROJECT_ID_NOT_DETECTED"

        bq_dataset_id = os.getenv("BQ_DATASET_ID", "BQ_DATASET_ID_NOT_DETECTED")

        return cls(
            run_id=run_id,
            # Workspace paths
            base_path=base_path,
            workspace_root=workspace_root,
            workspace_run_path=workspace_run_path,
            raw_snapshot_path=raw_snapshot_path,
            contracted_path=contracted_path,
            assembled_path=assembled_path,
            semantic_path=semantic_path,
            logs_path=logs_path,
            metadata_path=metadata_path,
            # Storage paths
            storage_raw_path=storage_raw_path,
            storage_contracted_path=storage_contracted_path,
            storage_mapping_path=storage_mapping_path,
            storage_published_path=storage_published_path,
            version_path=version_path,
            latest_pointer_path=latest_pointer_path,
            storage_runs_path=storage_runs_path,
            # BigQuery
            bq_project_id=bq_project_id,
            bq_dataset_id=bq_dataset_id,
        )

    def initialize_directories(self) -> None:
        """
        Physically instantiates the local workspace directory tree.

        Contract:
        - Creates the run-scoped folder structure on the local filesystem.
        - Idempotent: Does not raise errors if directories already exist.

        Side Effects:
        - Performs recursive directory creation (mkdir -p).
        """

        self.raw_snapshot_path.mkdir(parents=True, exist_ok=True)
        self.contracted_path.mkdir(parents=True, exist_ok=True)
        self.assembled_path.mkdir(parents=True, exist_ok=True)
        self.semantic_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
