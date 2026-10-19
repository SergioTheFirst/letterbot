"""Read-only SQLite connects must survive URI-special characters in the path.

``f"file:{path}?mode=ro"`` treats ``#`` as a fragment and ``%`` as an escape,
so a DB under such a directory either fails to open or opens the wrong file.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from mailbot_v26.observability.metrics import MetricsAggregator
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.web_observability import doctor_export
from mailbot_v26.web_observability.app import _open_readonly_connection


@pytest.fixture()
def special_db(tmp_path: Path) -> Path:
    directory = tmp_path / "letter#bot 100%"
    directory.mkdir()
    db_path = directory / "knowledge.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE marker (value TEXT)")
        conn.execute("INSERT INTO marker (value) VALUES ('ok')")
        conn.execute(
            "CREATE TABLE system_health_snapshots (payload_json TEXT, ts_utc TEXT)"
        )
        conn.execute(
            "INSERT INTO system_health_snapshots VALUES (?, ?)",
            (json.dumps({"status": "green"}), "2026-10-20T00:00:00+00:00"),
        )
    conn.close()
    return db_path


def _assert_readonly_marker(conn: sqlite3.Connection) -> None:
    try:
        assert conn.execute("SELECT value FROM marker").fetchone() == ("ok",)
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO marker (value) VALUES ('write')")
    finally:
        conn.close()


def test_metrics_connect_readonly_handles_special_path(special_db: Path) -> None:
    _assert_readonly_marker(MetricsAggregator(special_db)._connect_readonly())


def test_analytics_connect_readonly_handles_special_path(special_db: Path) -> None:
    analytics = KnowledgeAnalytics(special_db, read_only=True)
    _assert_readonly_marker(analytics._connect_readonly())


def test_web_readonly_connection_handles_special_path(special_db: Path) -> None:
    _assert_readonly_marker(_open_readonly_connection(special_db))


def test_doctor_health_snapshot_handles_special_path(special_db: Path) -> None:
    payload = doctor_export._latest_health_snapshot(special_db)
    assert payload.get("status") == "green"
