from __future__ import annotations

import json
from pathlib import Path

import pytest

from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.tests.fixtures.eml.fixture_builder import FIXTURE_SPECS
from mailbot_v26.tools import dry_run


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "eml"


def _norm(value: str) -> str:
    return " ".join(str(value or "").split()).casefold()


def test_dry_run_invoice_produces_correct_render(tmp_path: Path) -> None:
    result = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "invoice_simple.eml",
        storage_dir=tmp_path,
    )

    assert result.render.render_mode == "full"
    assert "12925" in result.render.text
    assert "INV-DRY-01" in result.render.text
    assert result.artifacts.interpretation.doc_kind == "invoice"


def test_dry_run_payroll_does_not_produce_invoice_render(tmp_path: Path) -> None:
    result = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "payroll_standard.eml",
        storage_dir=tmp_path,
    )

    assert result.render.render_mode == "full"
    assert result.artifacts.interpretation.doc_kind == "payroll"
    assert "Оплатить" not in result.render.text
    assert "invoice" not in _norm(result.render.text)


def test_dry_run_is_deterministic(tmp_path: Path) -> None:
    first = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "invoice_with_attachment.eml",
        storage_dir=tmp_path,
    )
    second = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "invoice_with_attachment.eml",
        storage_dir=tmp_path,
    )

    assert first.to_dict() == second.to_dict()


def test_dry_run_no_db_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "dry_run.sqlite"

    result = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "generic_notification.eml",
        storage_dir=tmp_path,
    )

    assert result.render.text
    assert not db_path.exists()


def test_dry_run_uses_real_pipeline_not_mocks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = {"parse": 0, "facts": 0}
    original_parse = dry_run.parse_raw_email
    original_collect = pipeline_processor._collect_message_facts

    def wrapped_parse(*args, **kwargs):
        calls["parse"] += 1
        return original_parse(*args, **kwargs)

    def wrapped_collect(*args, **kwargs):
        calls["facts"] += 1
        return original_collect(*args, **kwargs)

    monkeypatch.setattr(dry_run, "parse_raw_email", wrapped_parse)
    monkeypatch.setattr(
        pipeline_processor,
        "_collect_message_facts",
        wrapped_collect,
    )

    result = dry_run.run_dry_run_fixture(
        FIXTURE_DIR / "invoice_simple.eml",
        storage_dir=tmp_path,
    )

    assert calls == {"parse": 1, "facts": 1}
    assert result.stage_order == (
        "parse_raw_email",
        "collect_message_facts",
        "validate_message_facts",
        "score_message_facts",
        "consistency_check_message_facts",
        "detect_conversation_context",
        "build_message_decision",
        "build_message_interpretation",
        "render_email_notification",
    )


def test_dry_run_json_output_contains_all_pipeline_stages(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = dry_run.main(
        ["--fixture", str(FIXTURE_DIR / "invoice_simple.eml"), "--json"]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert "parse" in payload
    assert "classification" in payload
    assert "facts" in payload
    assert "validation" in payload
    assert "scoring" in payload
    assert "consistency" in payload
    assert "template" in payload
    assert "decision" in payload
    assert "interpretation" in payload
    assert "render" in payload


@pytest.mark.parametrize("spec", FIXTURE_SPECS, ids=lambda spec: spec.filename)
def test_dry_run_for_each_golden_corpus_eml_category(
    spec,
    tmp_path: Path,
) -> None:
    fixture_path = FIXTURE_DIR / spec.filename

    assert fixture_path.exists()
    result = dry_run.run_dry_run_fixture(fixture_path, storage_dir=tmp_path)

    assert result.render.render_mode == spec.expected_render_mode
    for token in spec.expected_contains:
        assert _norm(token) in _norm(result.render.text)
    for token in spec.expected_not_contains:
        assert _norm(token) not in _norm(result.render.text)
