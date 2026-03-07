# =============================================================================
# RUN CONTEXT (PATH CONSTRUCTION)
# =============================================================================

from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from datetime import datetime
import uuid


def _generate_run_id() -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    random_suffix = uuid.uuid4().hex[:6]
    return f"{timestamp}_{random_suffix}"


@dataclass
class RunContext:
    """
    Run-scoped execution context.

    Responsibilities:
    - Generate and hold run_id
    - Define all stage directory paths
    - Provide consistent path resolution across stages
    - Enforce run isolation via run-scoped folders

    Guarantees:
    - Each run writes only within its own directory tree
    - Published artifacts are resolved deterministically from run_id
    """

    run_id: str
    base_path: str | Path

    # Workspace execution paths
    workspace_root: Path
    workspace_run_path: Path

    # Storage paths
    storage_raw_path: Path
    storage_published_path: Path
    version_path: Path
    latest_pointer_path: Path

    # Run scope paths
    raw_snapshot_path: Path
    contracted_path: Path
    assembled_path: Path
    semantic_path: Path
    logs_path: Path
    metadata_path: Path

    # TODO: replace ./runtime to /tmp and ./data with gs://pipeline-bucket
    # before creating docker image.
    @classmethod
    def create(
        cls,
        base: str | Path = "./runtime",
        storage: str | Path = "./data",
        run_id: str | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ) -> "RunContext":

        base_path = Path(base)
        storage_path = Path(storage)

        if run_id is None:
            generator = run_id_factory or _generate_run_id
            run_id = generator()

        # Workspace paths
        workspace_root = base_path / "workspace"
        workspace_run_path = workspace_root / "runs" / run_id

        # Storage paths
        storage_root = storage_path
        storage_raw_path = storage_root / "raw"
        storage_published_path = storage_root / "published"
        version_path = storage_published_path / f"v{run_id}"
        latest_pointer_path = storage_published_path / "_latest.json"

        # Run scope paths
        raw_snapshot_path = workspace_run_path / "raw_snapshot"
        contracted_path = workspace_run_path / "contracted"
        assembled_path = workspace_run_path / "assembled"
        semantic_path = workspace_run_path / "semantic"
        logs_path = workspace_run_path / "logs"
        metadata_path = workspace_run_path / "metadata.json"

        return cls(
            run_id=run_id,
            base_path=base_path,
            workspace_root=workspace_root,
            workspace_run_path=workspace_run_path,
            storage_raw_path=storage_raw_path,
            storage_published_path=storage_published_path,
            raw_snapshot_path=raw_snapshot_path,
            contracted_path=contracted_path,
            assembled_path=assembled_path,
            semantic_path=semantic_path,
            logs_path=logs_path,
            metadata_path=metadata_path,
            version_path=version_path,
            latest_pointer_path=latest_pointer_path,
        )

    def initialize_directories(self) -> None:
        """
        Create run-scoped directories.

        Does not create published/version folders.
        """

        self.raw_snapshot_path.mkdir(parents=True, exist_ok=True)
        self.contracted_path.mkdir(parents=True, exist_ok=True)
        self.assembled_path.mkdir(parents=True, exist_ok=True)
        self.semantic_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
