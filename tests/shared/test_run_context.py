# =============================================================================
# UNIT TESTS FOR run_context.py
# =============================================================================

from data_pipeline.shared.run_context import RunContext


def test_run_context_paths_with_explicit_run_id(tmp_path):

    ctx = RunContext.create(
        base_path=tmp_path,
        run_id="test123",
    )

    assert ctx.run_id == "test123"
    assert ctx.run_path == tmp_path / "runs" / "test123"
    assert ctx.raw_snapshot_path == ctx.run_path / "raw_snapshot"
    assert ctx.version_path == tmp_path / "published" / "vtest123"


def test_run_context_generates_run_id(tmp_path):

    ctx = RunContext.create(base_path=tmp_path, run_id=None)

    assert ctx.run_id is not None
    assert "_" in ctx.run_id
    assert len(ctx.run_id.split("_")[1]) == 6


def test_initialize_directories_does_not_create_publish_dirs(tmp_path):

    ctx = RunContext.create(base_path=tmp_path, run_id="abc123")

    ctx.initialize_directories()

    assert not ctx.published_path.exists()
    assert not ctx.version_path.exists()


def test_initialize_directories_is_idempotent(tmp_path):

    ctx = RunContext.create(base_path=tmp_path, run_id="abc123")

    ctx.initialize_directories()
    ctx.initialize_directories()  # should not raise

    assert ctx.raw_snapshot_path.exists()


def test_run_id_format_is_stable(tmp_path):

    ctx = RunContext.create(
        base_path=tmp_path,
        run_id_factory=lambda: "20240102T030405_abcdef",
    )

    assert ctx.run_id == "20240102T030405_abcdef"


def test_source_raw_subpath_respected(tmp_path):

    ctx = RunContext.create(
        base_path=tmp_path,
        source_raw_subpath="incoming",
        run_id="abc",
    )

    assert ctx.source_raw_path == tmp_path / "incoming"


def test_default_run_id_shape(tmp_path):

    ctx = RunContext.create(base_path=tmp_path)

    assert "_" in ctx.run_id
    assert len(ctx.run_id.split("_")[1]) == 6


# =============================================================================
# UNIT TESTS END
# =============================================================================
