# =============================================================================
# UNIT TESTS FOR run_pipeline.py
# =============================================================================

from data_pipeline.shared.run_context import RunContext
from data_pipeline.run_pipeline import snapshot_raw, main
import pytest


def test_snapshot_raw_raises_when_source_missing(tmp_path):

    ctx = RunContext.create(base_path=tmp_path, run_id="x")

    with pytest.raises(FileNotFoundError):
        snapshot_raw(ctx)


def test_main_exits_on_validation_1_errors(monkeypatch, tmp_path):

    fake_ctx = RunContext.create(base_path=tmp_path, run_id="x")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: fake_ctx,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_validation",
        lambda *args, **kwargs: {
            "errors": ["boom"],  # force to fail on validation_1
            "warnings": [],
        },
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (fake_ctx.logs_path / "validation_initial.json").exists()


def test_main_exits_on_validation_2_issues(monkeypatch, tmp_path):

    fake_ctx = RunContext.create(base_path=tmp_path, run_id="x")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: fake_ctx,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.TABLE_CONFIG",
        {"dummy": {}},
    )

    # execution count
    calls = {"count": 0}

    def fake_validation(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return {"errors": [], "warnings": []}  # force to pass on validation_1
        return {"errors": [], "warnings": ["warn"]}  # fail on validation_2

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_validation",
        fake_validation,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_contract",
        lambda *a, **k: ({}, set()),
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.assemble_events",
        lambda *a, **k: {"status": "success", "error": [], "info": []},
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (fake_ctx.logs_path / "validation_post_contract.json").exists()


def test_main_success(monkeypatch, tmp_path):

    fake_ctx = RunContext.create(base_path=tmp_path, run_id="x")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: fake_ctx,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.TABLE_CONFIG",
        {"dummy": {}},
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_validation",
        lambda *a, **k: {
            "errors": [],
            "warnings": [],
        },  # Pass all validations
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_contract",
        lambda *a, **k: ({}, set()),
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.assemble_events",
        lambda *a, **k: {
            "status": "success",
            "error": [],
            "info": [],
        },  # Pass, status success
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.build_semantic_layer",
        lambda *a, **k: {
            "status": "success",
            "error": [],
            "info": [],
        },  # Pass, status success
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.run_integrity_gate",
        lambda *a, **k: {
            "status": "success",
            "errors": [],
            "info": [],
        },  # Pass, status success
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.promote_semantic_version",
        lambda *a, **k: {
            "status": "success",
            "errors": [],
            "info": [],
        },  # Pass, status success
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 0
    assert (fake_ctx.logs_path / "validation_initial.json").exists()
    assert (fake_ctx.logs_path / "contract_report.json").exists()
    assert (fake_ctx.logs_path / "validation_post_contract.json").exists()
    assert (fake_ctx.logs_path / "assemble_report.json").exists()
    assert (fake_ctx.logs_path / "semantic_report.json").exists()
    assert (fake_ctx.logs_path / "publish_integrity_report.json").exists()
    assert (fake_ctx.logs_path / "publish_promotion_report.json").exists()


# =============================================================================
# UNIT TESTS END
# =============================================================================
