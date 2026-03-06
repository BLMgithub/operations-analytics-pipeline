# =============================================================================
# UNIT TESTS FOR run_pipeline.py
# =============================================================================

from data_pipeline.shared.run_context import RunContext
from data_pipeline.run_pipeline import snapshot_raw, main
import pytest


def test_snapshot_raw_raises_when_source_missing(tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    with pytest.raises(FileNotFoundError):
        snapshot_raw(run_context)


def test_main_fails_on_initial_validation(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_validation",
        lambda *args, **kwargs: {
            "errors": ["boom"],  # force to fail on initial validation
            "warnings": [],
        },
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (run_context.logs_path / "validation_initial.json").exists()


def test_main_fails_on_post_contract_validation(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
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
            return {"errors": [], "warnings": []}
        return {
            "errors": [],
            "warnings": ["warn"],
        }  # force to pass on post contract validation

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_validation",
        fake_validation,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.apply_contract",
        lambda *a, **k: ({}, set()),
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (run_context.logs_path / "validation_post_contract.json").exists()


def test_main_fails_on_assemble_events(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
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
            "status": "failed",
            "error": ["boom"],
            "info": [],
        },  # Force to fail on assemble events
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (run_context.logs_path / "validation_initial.json").exists()
    assert (run_context.logs_path / "contract_report.json").exists()
    assert (run_context.logs_path / "validation_post_contract.json").exists()
    assert (run_context.logs_path / "assemble_report.json").exists()


def test_main_fails_on_build_semantic_layer(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
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
            "status": "failed",
            "error": ["boom"],
            "info": [],
        },  # Force to fail on build semantic layer
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (run_context.logs_path / "validation_initial.json").exists()
    assert (run_context.logs_path / "contract_report.json").exists()
    assert (run_context.logs_path / "validation_post_contract.json").exists()
    assert (run_context.logs_path / "assemble_report.json").exists()
    assert (run_context.logs_path / "semantic_report.json").exists()


def test_main_fails_on_execute_publish_lifecycle(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
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
        "data_pipeline.run_pipeline.execute_publish_lifecycle",
        lambda *a, **k: {
            "status": "failed",
            "errors": ["boom"],
            "info": [],
        },  # Force to fail on publish lifecyle
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 1
    assert (run_context.logs_path / "validation_initial.json").exists()
    assert (run_context.logs_path / "contract_report.json").exists()
    assert (run_context.logs_path / "validation_post_contract.json").exists()
    assert (run_context.logs_path / "assemble_report.json").exists()
    assert (run_context.logs_path / "semantic_report.json").exists()
    assert (run_context.logs_path / "publish_report.json").exists()


def test_main_success(monkeypatch, tmp_path):

    run_context = RunContext.create(base_path=tmp_path, run_id="20230101T000000_abc123")

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.snapshot_raw",
        lambda *_: None,
    )

    monkeypatch.setattr(
        "data_pipeline.run_pipeline.RunContext.create",
        lambda: run_context,
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
        "data_pipeline.run_pipeline.execute_publish_lifecycle",
        lambda *a, **k: {
            "status": "success",
            "errors": [],
            "info": [],
        },  # Pass, status success
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 0
    assert (run_context.logs_path / "validation_initial.json").exists()
    assert (run_context.logs_path / "contract_report.json").exists()
    assert (run_context.logs_path / "validation_post_contract.json").exists()
    assert (run_context.logs_path / "assemble_report.json").exists()
    assert (run_context.logs_path / "semantic_report.json").exists()
    assert (run_context.logs_path / "publish_report.json").exists()


# =============================================================================
# UNIT TESTS END
# =============================================================================
