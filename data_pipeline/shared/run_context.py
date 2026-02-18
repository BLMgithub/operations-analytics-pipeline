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
    run_id: str
    base_path: str | Path

    # root & source
    run_path: Path
    source_raw_path: Path

    # Run scope paths
    raw_snapshot_path: Path
    contracted_path: Path
    assembled_path: Path
    semantic_path: Path
    logs_path: Path
    metadata_path: Path

    # Publish paths
    published_path: Path
    version_path: Path
    latest_pointer_path: Path

    @classmethod
    def create(
        cls,
        base_path: str | Path = "data",
        source_raw_subpath: str = "raw",
        run_id: str | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ) -> "RunContext":

        base = Path(base_path)

        if run_id is None:
            generator = run_id_factory or _generate_run_id
            run_id = generator()

        run_path = base / "runs" / run_id
        source_raw_path = base / source_raw_subpath

        raw_snapshot_path = run_path / "raw_snapshot"
        contracted_path = run_path / "contracted"
        assembled_path = run_path / "assembled"
        semantic_path = run_path / "semantic"
        logs_path = run_path / "logs"
        metadata_path = run_path / "metadata.json"

        published_path = base / "published"
        version_path = published_path / f"v{run_id}"
        latest_pointer_path = published_path / "_latest.json"

        return cls(
            run_id=run_id,
            base_path=base,
            run_path=run_path,
            source_raw_path=source_raw_path,
            raw_snapshot_path=raw_snapshot_path,
            contracted_path=contracted_path,
            assembled_path=assembled_path,
            semantic_path=semantic_path,
            logs_path=logs_path,
            metadata_path=metadata_path,
            published_path=published_path,
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
