from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.tools.export_data import export_data


def _insert_processing_span(
    db_path: Path,
    *,
    span_id: str,
    account_id: str,
    ts_start_utc: float,
    total_duration_ms: int,
    llm_latency_ms: int | None,
    llm_quality_score: float | None,
    fallback_used: bool,
    outcome: str,
    error_code: str = "",
    stage_durations: dict[str, int] | None = None,
    health_snapshot_id: str = "",
) -> None:
    ts_end_utc = ts_start_utc + (total_duration_ms / 1000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO processing_spans (
                span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
                stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
                fallback_used, outcome, error_code, health_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, ?, ?)
            """,
            (
                span_id,
                ts_start_utc,
                ts_end_utc,
                total_duration_ms,
                account_id,
                json.dumps(stage_durations or {}, ensure_ascii=False),
                llm_latency_ms,
                llm_quality_score,
                1 if fallback_used else 0,
                outcome,
                error_code,
                health_snapshot_id,
            ),
        )


def _insert_health_snapshot(
    db_path: Path,
    *,
    snapshot_id: str,
    ts_utc: float,
    gates_state: str,
    metrics_brief: str,
    system_mode: str,
    payload_json: str = "{}",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO system_health_snapshots (
                snapshot_id, ts_utc, payload_json, gates_state, metrics_brief, system_mode
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                ts_utc,
                payload_json,
                gates_state,
                metrics_brief,
                system_mode,
            ),
        )


def test_processing_spans_metrics_scoped(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    base_ts = datetime.now(timezone.utc) - timedelta(days=1)
    _insert_processing_span(
        db_path,
        span_id="s1",
        account_id="a-primary",
        ts_start_utc=base_ts.timestamp(),
        total_duration_ms=100,
        llm_latency_ms=50,
        llm_quality_score=0.8,
        fallback_used=False,
        outcome="ok",
        stage_durations={"parse": 10},
    )
    _insert_processing_span(
        db_path,
        span_id="s2",
        account_id="a-secondary",
        ts_start_utc=base_ts.timestamp() + 1,
        total_duration_ms=300,
        llm_latency_ms=150,
        llm_quality_score=0.6,
        fallback_used=True,
        outcome="error",
        error_code="timeout",
        stage_durations={"llm": 120},
    )
    _insert_processing_span(
        db_path,
        span_id="s3",
        account_id="a-secondary",
        ts_start_utc=base_ts.timestamp() + 2,
        total_duration_ms=500,
        llm_latency_ms=250,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        stage_durations={"final": 50},
    )

    analytics = KnowledgeAnalytics(db_path)
    digest = analytics.processing_spans_metrics_digest(
        account_email="a-primary",
        account_emails=["a-primary", "a-secondary"],
        window_days=7,
    )

    assert digest["span_count"] == 3
    assert round(digest["total_duration_ms_p50"], 2) == 300
    assert round(digest["total_duration_ms_p90"], 2) >= 460
    assert round(digest["llm_latency_ms_p50"], 2) == 150
    assert round(digest["llm_latency_ms_p90"], 2) >= 230
    assert digest["llm_quality_avg"] == pytest.approx((0.8 + 0.6) / 2)
    assert digest["error_rate"] == pytest.approx(1 / 3)
    assert digest["fallback_rate"] == pytest.approx(1 / 3)
    assert digest["outcome_counts"] == {"ok": 2, "error": 1}


def test_processing_spans_scope_fallback_empty_list(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    now_ts = datetime.now(timezone.utc).timestamp()
    _insert_processing_span(
        db_path,
        span_id="s4",
        account_id="only-primary",
        ts_start_utc=now_ts,
        total_duration_ms=200,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
    )

    analytics = KnowledgeAnalytics(db_path)
    digest = analytics.processing_spans_metrics_digest(
        account_email="only-primary", account_emails=[], window_days=3
    )

    assert digest["span_count"] == 1
    assert digest["llm_latency_ms_p50"] is None
    assert digest["llm_quality_avg"] is None


def test_processing_spans_recent_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    now_ts = datetime.now(timezone.utc).timestamp()
    _insert_processing_span(
        db_path,
        span_id="err1",
        account_id="err-account",
        ts_start_utc=now_ts - 10,
        total_duration_ms=120,
        llm_latency_ms=60,
        llm_quality_score=None,
        fallback_used=True,
        outcome="error",
        error_code="llm_failed",
        stage_durations={"llm": 55},
    )
    _insert_processing_span(
        db_path,
        span_id="ok1",
        account_id="err-account",
        ts_start_utc=now_ts - 5,
        total_duration_ms=80,
        llm_latency_ms=40,
        llm_quality_score=0.5,
        fallback_used=False,
        outcome="ok",
    )
    _insert_processing_span(
        db_path,
        span_id="err2",
        account_id="err-account",
        ts_start_utc=now_ts,
        total_duration_ms=90,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="error",
        error_code="timeout",
        stage_durations={"parse": 15},
    )

    analytics = KnowledgeAnalytics(db_path)
    errors = analytics.processing_spans_recent_errors(
        account_email="err-account", account_emails=None, window_days=2, limit=5
    )

    assert [item["span_id"] for item in errors] == ["err2", "err1"]
    assert all("stage_durations" in item for item in errors)
    assert errors[0]["error_code"] == "timeout"


def test_health_current_scoped(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    base_ts = datetime.now(timezone.utc).timestamp()
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-a",
        ts_utc=base_ts - 50,
        gates_state=json.dumps({"gate_a": "open"}),
        metrics_brief=json.dumps({"shadow_accuracy": 0.9}),
        system_mode="normal",
    )
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-b",
        ts_utc=base_ts - 10,
        gates_state=json.dumps({"gate_b": "closed"}),
        metrics_brief=json.dumps({"llm_failure_rate": 0.1}),
        system_mode="degraded",
    )
    _insert_processing_span(
        db_path,
        span_id="span-a",
        account_id="a-primary",
        ts_start_utc=base_ts - 40,
        total_duration_ms=120,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-a",
    )
    _insert_processing_span(
        db_path,
        span_id="span-b",
        account_id="a-secondary",
        ts_start_utc=base_ts - 5,
        total_duration_ms=140,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-b",
    )
    _insert_processing_span(
        db_path,
        span_id="span-c",
        account_id="other",
        ts_start_utc=base_ts - 1,
        total_duration_ms=100,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-b",
    )

    analytics = KnowledgeAnalytics(db_path)
    current = analytics.processing_spans_health_current(
        account_email="a-primary",
        account_emails=["a-primary", "a-secondary"],
        window_days=7,
    )

    assert current is not None
    assert current["snapshot_id"] == "snap-b"
    assert current["system_mode"] == "degraded"
    assert current["gates_state"].get("gate_b") == "closed"


def test_health_timeline_order_and_limit(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    base_ts = datetime.now(timezone.utc).timestamp()
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-1",
        ts_utc=base_ts - 60,
        gates_state=json.dumps({"gate": "on"}),
        metrics_brief=json.dumps({"shadow_accuracy": 0.8}),
        system_mode="normal",
    )
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-2",
        ts_utc=base_ts - 20,
        gates_state=json.dumps({"gate": "off"}),
        metrics_brief=json.dumps({"shadow_accuracy": 0.7}),
        system_mode="safe",
    )
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-3",
        ts_utc=base_ts - 5,
        gates_state=json.dumps({"gate": "warn"}),
        metrics_brief=json.dumps({"shadow_accuracy": 0.6}),
        system_mode="alert",
    )
    _insert_processing_span(
        db_path,
        span_id="t1",
        account_id="acct",
        ts_start_utc=base_ts - 55,
        total_duration_ms=100,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-1",
    )
    _insert_processing_span(
        db_path,
        span_id="t2",
        account_id="acct",
        ts_start_utc=base_ts - 15,
        total_duration_ms=100,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-2",
    )
    _insert_processing_span(
        db_path,
        span_id="t3",
        account_id="acct",
        ts_start_utc=base_ts - 1,
        total_duration_ms=100,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-3",
    )

    analytics = KnowledgeAnalytics(db_path)
    timeline = analytics.processing_spans_health_timeline(
        account_email="acct",
        account_emails=None,
        window_days=7,
        limit=2,
    )

    assert [item["snapshot_id"] for item in timeline] == ["snap-3", "snap-2"]
    assert timeline[0]["system_mode"] == "alert"


def test_health_json_parse_failure_safe(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    now_ts = datetime.now(timezone.utc).timestamp()
    _insert_health_snapshot(
        db_path,
        snapshot_id="snap-bad",
        ts_utc=now_ts,
        gates_state="{not-json}",
        metrics_brief="{not-json either}",
        system_mode="normal",
    )
    _insert_processing_span(
        db_path,
        span_id="span-bad",
        account_id="acct",
        ts_start_utc=now_ts,
        total_duration_ms=50,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        health_snapshot_id="snap-bad",
    )

    analytics = KnowledgeAnalytics(db_path)
    current = analytics.processing_spans_health_current(
        account_email="acct", account_emails=None, window_days=7
    )

    assert current is not None
    assert current["gates_state"] == {}
    assert current["metrics_brief"] == {}


def test_export_includes_processing_and_health(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    recorder = ProcessingSpanRecorder(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS events_v1 (
                id INTEGER PRIMARY KEY,
                event_type TEXT,
                ts_utc REAL,
                ts TEXT,
                account_id TEXT,
                entity_id TEXT,
                email_id INTEGER,
                payload TEXT,
                schema_version INTEGER,
                fingerprint TEXT
            );
            CREATE TABLE IF NOT EXISTS commitments (
                id INTEGER PRIMARY KEY,
                email_row_id INTEGER,
                source TEXT,
                commitment_text TEXT,
                deadline_iso TEXT,
                status TEXT,
                confidence REAL,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS relationship_health_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                created_at TEXT,
                entity_id TEXT,
                health_score REAL,
                reason TEXT,
                components_breakdown TEXT,
                data_window_days INTEGER
            );
            """)
        conn.execute(
            "INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id, email_id, payload, schema_version, fingerprint)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "test_event",
                datetime.now(timezone.utc).timestamp(),
                "",
                "acc",
                "ent",
                1,
                "{}",
                1,
                "fp",
            ),
        )
        conn.execute(
            "INSERT INTO commitments (email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "src",
                "do thing",
                "",
                "open",
                0.5,
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.execute(
            "INSERT INTO relationship_health_snapshots (snapshot_id, created_at, entity_id, health_score, reason, components_breakdown, data_window_days)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "snap-1",
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "ent",
                0.8,
                "ok",
                "{}",
                7,
            ),
        )
        conn.commit()

    span = recorder.start(account_id="acc", email_id=1)
    span.record_stage("parse", 5)
    recorder.finalize(
        span,
        llm_provider=None,
        llm_model=None,
        llm_latency_ms=None,
        llm_quality_score=None,
        fallback_used=False,
        outcome="ok",
        error_code="",
        health_snapshot_payload={"metrics": {"days_1": {"shadow_accuracy": 1.0}}},
    )

    output_path = tmp_path / "export.jsonl"
    since_dt = datetime.now(timezone.utc) - timedelta(days=3)
    result = export_data(db_path=db_path, output_path=output_path, since_dt=since_dt)

    assert result.output_path == output_path
    assert output_path.exists()
    with output_path.open() as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    record_types = {line.get("record_type") for line in lines}
    assert "processing_span" in record_types
    assert "system_health_snapshot" in record_types
