import json
import sqlite3
import time
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder


def _build_span(
    recorder: ProcessingSpanRecorder,
    *,
    account: str,
    email_id: int | None,
    payload: dict,
    outcome: str = "ok",
    error_code: str = "",
    fallback: bool = False,
) -> None:
    span = recorder.start(account_id=account, email_id=email_id)
    time.sleep(0.001)
    span.record_stage("parse", 5)
    recorder.finalize(
        span,
        llm_provider=None,
        llm_model=None,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=fallback,
        outcome=outcome,
        error_code=error_code,
        health_snapshot_payload=payload,
    )


def test_snapshot_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    recorder = ProcessingSpanRecorder(db_path)
    payload = {
        "metrics": {"days_7": {"shadow_accuracy": 0.5}},
        "gates": {"passed": True, "failed": []},
    }

    _build_span(recorder, account="acc", email_id=1, payload=payload)
    _build_span(recorder, account="acc", email_id=2, payload=payload)

    with sqlite3.connect(db_path) as conn:
        span_count = conn.execute("SELECT COUNT(*) FROM processing_spans").fetchone()[0]
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM system_health_snapshots"
        ).fetchone()[0]

    assert span_count == 2
    assert snapshot_count == 1


def test_health_snapshot_pii_guard(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    recorder = ProcessingSpanRecorder(db_path)
    payload = {
        "subject": "secret subject",
        "body": "full body",
        "telegram_text": "text",
        "rendered_message": "render",
        "digest_text": "digest",
        "metrics": {"days_7": {"shadow_accuracy": 0.7}},
        "gates": {"passed": False, "failed": ["llm_failure_rate"]},
    }

    _build_span(recorder, account="acc", email_id=3, payload=payload)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT payload_json FROM system_health_snapshots LIMIT 1"
        ).fetchone()
    stored = json.loads(row[0])
    banned_keys = {"subject", "body", "telegram_text", "rendered_message", "digest_text"}
    assert not banned_keys.intersection(stored.keys())


def test_processing_span_basic_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    recorder = ProcessingSpanRecorder(db_path)
    payload = {"metrics": {"days_7": {"shadow_accuracy": 0.9}}, "gates": {"passed": True, "failed": []}}

    span = recorder.start(account_id="acc", email_id=4)
    time.sleep(0.002)
    span.record_stage("llm", 12)
    recorder.finalize(
        span,
        llm_provider="gigachat",
        llm_model="gpt-test",
        llm_latency_ms=120,
        llm_quality_score=0.85,
        fallback_used=False,
        outcome="ok",
        error_code="",
        health_snapshot_payload=payload,
    )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM processing_spans LIMIT 1").fetchone()

    assert row["total_duration_ms"] > 0
    assert row["outcome"] == "ok"
    durations = json.loads(row["stage_durations_json"])
    assert isinstance(durations, dict)
    assert durations.get("llm") == 12
    assert row["llm_quality_score"] == 0.85
