"""The dashboard cache is keyed by the resolved scope (P3b D1/C7).

/api/dashboard falls back to the account scope remembered in the session,
but its cache key only looked at the query string: while the database did
not change, a request without ?account_emails= was served whatever scope
was cached last, from any session.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import create_app

ALPHA = "alpha@example.org"
ZULU = "zulu@example.org"


@pytest.fixture(autouse=True)
def _quiet_database(monkeypatch: pytest.MonkeyPatch) -> None:
    # Readers may bump the -shm mtime in WAL mode; pin the change token so
    # the cache behaves as it does on a database nobody is writing to.
    monkeypatch.setattr(web_app, "_db_change_token", lambda db_path: ("quiet",))


def _app(tmp_path: Path):
    db_path = tmp_path / "cache-scope.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        for account in (ALPHA, ZULU, ZULU):
            conn.execute(
                "INSERT INTO emails (account_email, from_email, subject, received_at, "
                "created_at) VALUES (?, 'x@sender.example', 's', ?, ?)",
                (account, now, now),
            )
        conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_sessions_with_different_scopes_get_their_own_counts(tmp_path: Path) -> None:
    app = _app(tmp_path)
    alpha_client = app.test_client()
    zulu_client = app.test_client()
    alpha_client.get("/", query_string={"account_emails": ALPHA})
    zulu_client.get("/", query_string={"account_emails": ZULU})

    alpha = alpha_client.get("/api/dashboard").get_json()
    zulu = zulu_client.get("/api/dashboard").get_json()

    assert alpha["emails_today"] == 1
    assert zulu["emails_today"] == 2


def test_changing_the_remembered_scope_refreshes_the_dashboard(tmp_path: Path) -> None:
    app = _app(tmp_path)
    client = app.test_client()
    client.get("/", query_string={"account_emails": ALPHA})
    first = client.get("/api/dashboard").get_json()
    # Any page remembers the scope; the archive does not build the dashboard.
    client.get("/archive", query_string={"account_emails": ZULU})
    second = client.get("/api/dashboard").get_json()

    assert first["emails_today"] == 1
    assert second["emails_today"] == 2
