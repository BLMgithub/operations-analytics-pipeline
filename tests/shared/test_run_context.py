# =============================================================================
# UNIT TESTS FOR run_context.py
# =============================================================================

from data_pipeline.shared.run_context import RunContext


def test_run_context_paths_with_explicit_run_id(tmp_path):

    ctx = RunContext.create(
        base=tmp_path,
        run_id="test123",
    )

    assert ctx.run_id == "test123"
    assert ctx.workspace_run_path == tmp_path / "workspace" / "runs" / "test123"
    assert ctx.raw_snapshot_path == ctx.workspace_run_path / "raw_snapshot"
    assert ctx.version_path == ctx.storage_published_path / "vtest123"


def test_run_context_generates_run_id(tmp_path):

    ctx = RunContext.create(base=tmp_path, run_id=None)

    assert ctx.run_id is not None
    assert "_" in ctx.run_id
    assert len(ctx.run_id.split("_")[1]) == 6


def test_initialize_directories_does_not_create_publish_dirs(tmp_path):

    ctx = RunContext.create(base=tmp_path, storage=tmp_path, run_id="abc123")

    ctx.initialize_directories()

    assert not ctx.storage_published_path.exists()
    assert not ctx.version_path.exists()


def test_initialize_directories_is_idempotent(tmp_path):

    ctx = RunContext.create(base=tmp_path, storage=tmp_path, run_id="abc123")

    ctx.initialize_directories()
    ctx.initialize_directories()  # should not raise

    assert ctx.raw_snapshot_path.exists()


def test_run_id_format_is_stable(tmp_path):

    ctx = RunContext.create(
        base=tmp_path,
        storage=tmp_path,
        run_id_factory=lambda: "20240102T030405_abcdef",
    )

    assert ctx.run_id == "20240102T030405_abcdef"


def test_default_run_id_shape(tmp_path):

    ctx = RunContext.create(base=tmp_path, storage=tmp_path)

    assert "_" in ctx.run_id
    assert len(ctx.run_id.split("_")[1]) == 6


# =============================================================================
# UNIT TESTS END
# =============================================================================
