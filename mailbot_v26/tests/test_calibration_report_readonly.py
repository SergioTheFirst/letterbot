"""QW17 (finding C10): calibration report reads the DB read-only and stays fresh."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability import calibration_report
from mailbot_v26.observability.calibration_report import (
    compute_priority_calibration_report,
)
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests.test_calibration_report import _emit_trace

NOW = datetime(2026, 1, 20, tzinfo=timezone.utc)


def _db(tmp_path: Path) -> tuple[Path, ContractEventEmitter]:
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    db_path = db_dir / "calibration.sqlite"
    KnowledgeDB(db_path)
    return db_path, ContractEventEmitter(db_path)


def _trace(emitter: ContractEventEmitter, email_id: int, hours_ago: float) -> None:
    _emit_trace(
        emitter,
        email_id=email_id,
        ts_utc=(NOW - timedelta(hours=hours_ago)).timestamp(),
        model_fingerprint="model-a",
        decision_key=f"key-{email_id}",
    )


def test_report_opens_db_read_only(tmp_path: Path, monkeypatch) -> None:
    db_path, emitter = _db(tmp_path)
    _trace(emitter, 1, hours_ago=2)
    real_connect = sqlite3.connect
    calls: list[tuple[object, dict[str, object]]] = []

    def recording_connect(database, *args, **kwargs):
        calls.append((database, dict(kwargs)))
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(calibration_report.sqlite3, "connect", recording_connect)
    report = compute_priority_calibration_report(
        db_path=db_path, days=7, now_ts_utc=NOW.timestamp()
    )
    assert report["totals"]["decisions_total"] == 1
    assert calls
    for database, kwargs in calls:
        assert kwargs.get("uri") is True
        assert str(database).startswith("file:")
        assert str(database).endswith("?mode=ro")


def test_report_does_not_create_missing_db(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    missing = tmp_path / "absent" / "nothing.sqlite"
    missing.parent.mkdir()
    report = compute_priority_calibration_report(
        db_path=missing, days=7, now_ts_utc=NOW.timestamp()
    )
    assert report["totals"]["decisions_total"] == 0
    assert not missing.exists()


def test_new_events_invalidate_cached_report(tmp_path: Path) -> None:
    db_path, emitter = _db(tmp_path)
    _trace(emitter, 1, hours_ago=5)
    first = compute_priority_calibration_report(
        db_path=db_path, days=7, now_ts_utc=NOW.timestamp()
    )
    assert first["totals"]["decisions_total"] == 1

    _trace(emitter, 2, hours_ago=1)
    second = compute_priority_calibration_report(
        db_path=db_path, days=7, now_ts_utc=NOW.timestamp()
    )
    assert second["totals"]["decisions_total"] == 2


def test_cache_file_lives_next_to_db_not_cwd(tmp_path: Path, monkeypatch) -> None:
    db_path, emitter = _db(tmp_path)
    _trace(emitter, 1, hours_ago=2)
    cwd = tmp_path / "elsewhere"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    compute_priority_calibration_report(
        db_path=db_path, days=7, now_ts_utc=NOW.timestamp()
    )
    assert (db_path.parent / "logs" / "priority_calibration_cache.json").exists()
    assert not (cwd / "logs").exists()
