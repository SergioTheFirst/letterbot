from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType
from mailbot_v26.tools import cleanup


def _schema_sql() -> str:
    return (Path(__file__).resolve().parents[1] / "storage" / "schema.sql").read_text(
        encoding="utf-8"
    )


def _init_cleanup_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(_schema_sql())
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trust_snapshots (
                id TEXT PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                entity_id TEXT NOT NULL,
                trust_score REAL,
                reason TEXT,
                sample_size INTEGER,
                data_quality TEXT,
                model_version TEXT DEFAULT 'v1'
            );
            CREATE TABLE IF NOT EXISTS relationship_health_snapshots (
                id TEXT PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                entity_id TEXT NOT NULL,
                health_score REAL,
                reason TEXT,
                components_breakdown JSON,
                data_window_days INTEGER
            );
            """
        )
        conn.commit()


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    fingerprint: str,
) -> None:
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type,
            ts_utc,
            ts,
            account_id,
            entity_id,
            email_id,
            payload,
            payload_json,
            schema_version,
            fingerprint
        ) VALUES (?, ?, ?, 'acc', NULL, NULL, '{}', '{}', 1, ?)
        """,
        (event_type, ts_utc, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_utc)), fingerprint),
    )


def _insert_decision_trace(
    conn: sqlite3.Connection, trace_id: str, created_at: str
) -> None:
    conn.execute(
        """
        INSERT INTO decision_traces (
            id,
            created_at,
            email_id,
            account_email,
            signal_entropy,
            signal_printable_ratio,
            signal_quality_score,
            signal_fallback_used,
            llm_provider,
            llm_model,
            prompt_full,
            response_full,
            priority,
            action_line,
            confidence,
            shadow_priority,
            compressed
        ) VALUES (?, ?, '1', 'acc@example.com', 0.1, 1.0, 1.0, 0, NULL, NULL, '', '', 'high', 'Review', 0.9, '', 0)
        """,
        (trace_id, created_at),
    )


def _insert_trust_snapshot(
    conn: sqlite3.Connection, snapshot_id: str, created_at: str
) -> None:
    conn.execute(
        """
        INSERT INTO trust_snapshots (
            id,
            created_at,
            entity_id,
            trust_score,
            reason,
            sample_size,
            data_quality,
            model_version
        ) VALUES (?, ?, 'entity', 0.5, 'ok', 5, 'good', 'v1')
        """,
        (snapshot_id, created_at),
    )


def _insert_relationship_snapshot(
    conn: sqlite3.Connection, snapshot_id: str, created_at: str
) -> None:
    conn.execute(
        """
        INSERT INTO relationship_health_snapshots (
            id,
            created_at,
            entity_id,
            health_score,
            reason,
            components_breakdown,
            data_window_days
        ) VALUES (?, ?, 'entity', 0.5, 'ok', '{}', 30)
        """,
        (snapshot_id, created_at),
    )


def _prepare_db(path: Path) -> None:
    if path.exists():
        path.unlink()
    _init_cleanup_db(path)
    old_ts = time.time() - 120 * 86400
    recent_ts = time.time() - 5 * 86400
    old_iso = "2000-01-01T00:00:00+00:00"
    recent_iso = "2099-01-01T00:00:00+00:00"
    with sqlite3.connect(path) as conn:
        _insert_event(
            conn,
            event_type=EventType.IMAP_HEALTH.value,
            ts_utc=old_ts,
            fingerprint="eligible-old",
        )
        _insert_event(
            conn,
            event_type=EventType.IMAP_HEALTH.value,
            ts_utc=recent_ts,
            fingerprint="eligible-new",
        )
        _insert_event(
            conn,
            event_type=EventType.MESSAGE_INTERPRETATION.value,
            ts_utc=old_ts,
            fingerprint="protected-old",
        )
        _insert_decision_trace(conn, "trace-old", old_iso)
        _insert_decision_trace(conn, "trace-new", recent_iso)
        _insert_trust_snapshot(conn, "trust-old", old_iso)
        _insert_trust_snapshot(conn, "trust-new", recent_iso)
        _insert_relationship_snapshot(conn, "rel-old", old_iso)
        _insert_relationship_snapshot(conn, "rel-new", recent_iso)
        conn.execute(
            """
            INSERT INTO priority_feedback (
                id,
                email_id,
                kind,
                value,
                entity_id,
                sender_email,
                account_email,
                created_at
            ) VALUES ('feedback-1', '1', 'priority', 'high', 'entity', 'sender@example.com', 'acc@example.com', ?)
            """,
            (old_iso,),
        )
        conn.commit()


def _count(conn: sqlite3.Connection, query: str, *params: object) -> int:
    row = conn.execute(query, params).fetchone()
    return int(row[0]) if row else 0


def test_dry_run_deletes_nothing(tmp_path: Path) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    result = cleanup.run_cleanup(db_path=db_path, run=False, status_only=False)

    assert result.mode == "dry-run"
    assert result.deleted_rows == 0
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "SELECT COUNT(*) FROM events_v1") == 3
        assert _count(conn, "SELECT COUNT(*) FROM decision_traces") == 2


def test_dry_run_prints_what_would_be_deleted(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    exit_code = cleanup.main(["--db", str(db_path)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Mode: dry-run" in output
    assert "events_v1:imap_health_v1" in output
    assert "decision_traces:decision_traces" in output
    assert "eligible=" in output


def test_run_mode_deletes_eligible_records(tmp_path: Path) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    result = cleanup.run_cleanup(db_path=db_path, run=True, status_only=False)

    assert result.mode == "run"
    assert result.deleted_rows >= 4
    with sqlite3.connect(db_path) as conn:
        assert _count(
            conn,
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = ?",
            EventType.IMAP_HEALTH.value,
        ) == 1
        assert _count(conn, "SELECT COUNT(*) FROM decision_traces") == 1
        assert _count(conn, "SELECT COUNT(*) FROM trust_snapshots") == 1
        assert _count(conn, "SELECT COUNT(*) FROM relationship_health_snapshots") == 1


def test_run_mode_never_deletes_protected_event_types(tmp_path: Path) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    cleanup.run_cleanup(db_path=db_path, run=True, status_only=False)

    with sqlite3.connect(db_path) as conn:
        assert _count(
            conn,
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = ?",
            EventType.MESSAGE_INTERPRETATION.value,
        ) == 1


def test_run_mode_never_deletes_protected_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    cleanup.run_cleanup(db_path=db_path, run=True, status_only=False)

    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "SELECT COUNT(*) FROM priority_feedback") == 1


def test_cleanup_uses_transaction_rollback_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)
    original_delete_batch = cleanup._delete_batch
    calls = {"count": 0}

    def flaky_delete_batch(
        conn: sqlite3.Connection,
        target: cleanup.CleanupTarget,
        *,
        batch_size: int,
    ) -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            return original_delete_batch(conn, target, batch_size=batch_size)
        raise RuntimeError("boom")

    monkeypatch.setattr(cleanup, "_delete_batch", flaky_delete_batch)

    with pytest.raises(RuntimeError, match="boom"):
        cleanup.run_cleanup(db_path=db_path, run=True, status_only=False, batch_size=1)

    with sqlite3.connect(db_path) as conn:
        assert _count(
            conn,
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = ?",
            EventType.IMAP_HEALTH.value,
        ) == 2


def test_status_mode_shows_counts_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)

    exit_code = cleanup.main(["--status", "--db", str(db_path)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Mode: status" in output
    assert "Deleted rows:" not in output
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "SELECT COUNT(*) FROM decision_traces") == 2


def test_cleanup_without_where_clause_is_impossible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)
    target = cleanup.resolve_cleanup_targets()[0]

    monkeypatch.setattr(cleanup, "_build_delete_sql", lambda _target: "DELETE FROM events_v1")

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(ValueError, match="without WHERE"):
            cleanup._delete_batch(conn, target, batch_size=10)


def test_retention_config_cannot_override_protected_types() -> None:
    allowed = cleanup.resolve_event_cleanup_types(
        [
            EventType.IMAP_HEALTH.value,
            EventType.MESSAGE_INTERPRETATION.value,
            "not_real",
        ]
    )

    assert allowed == (EventType.IMAP_HEALTH.value,)
    assert EventType.MESSAGE_INTERPRETATION.value not in allowed


def test_vacuum_not_run_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)
    called = {"value": False}

    def fake_vacuum(_db_path: Path) -> None:
        called["value"] = True

    monkeypatch.setattr(cleanup, "_run_vacuum", fake_vacuum)

    result = cleanup.run_cleanup(
        db_path=db_path,
        run=True,
        status_only=False,
        vacuum=False,
        vacuum_threshold_rows=999,
    )

    assert called["value"] is False
    assert result.vacuum_performed is False
    assert result.vacuum_reason == "not_requested"


def test_vacuum_runs_only_when_explicitly_requested_or_threshold_hit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "cleanup.sqlite"
    _prepare_db(db_path)
    calls: list[Path] = []

    def fake_vacuum(path: Path) -> None:
        calls.append(path)

    monkeypatch.setattr(cleanup, "_run_vacuum", fake_vacuum)

    explicit = cleanup.run_cleanup(
        db_path=db_path,
        run=True,
        status_only=False,
        vacuum=True,
        vacuum_threshold_rows=999,
    )

    assert explicit.vacuum_performed is True
    assert explicit.vacuum_reason == "explicit"
    assert calls == [db_path]

    _prepare_db(db_path)
    calls.clear()
    threshold = cleanup.run_cleanup(
        db_path=db_path,
        run=True,
        status_only=False,
        vacuum=False,
        vacuum_threshold_rows=1,
    )

    assert threshold.vacuum_performed is True
    assert threshold.vacuum_reason == "threshold"
    assert calls == [db_path]
