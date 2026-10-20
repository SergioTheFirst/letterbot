"""Dashboard counts honour their time windows (P3b D1/C3).

SQLite strftime('%s', ...) returns TEXT, and TEXT compares greater than any
REAL bound, so "emails in the last 24 h" used to count every row ever stored.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"


def _insert_email(
    conn: sqlite3.Connection, *, sender: str, received: datetime, priority: str = "🔴"
) -> None:
    conn.execute(
        """
        INSERT INTO emails (account_email, from_email, subject, priority,
                            llm_provider, received_at, created_at)
        VALUES (?, ?, 'Subject', ?, 'gigachat', ?, ?)
        """,
        (ACCOUNT, sender, priority, received.isoformat(), received.isoformat()),
    )


def _seed_windows_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "windows.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for offset in (
            timedelta(minutes=10),
            timedelta(hours=3),
            timedelta(days=3),
            timedelta(days=40),
        ):
            _insert_email(conn, sender="alice@example.com", received=now - offset)
        for email_id, offset in (("1", timedelta(days=1)), ("2", timedelta(days=10))):
            conn.execute(
                """
                INSERT INTO priority_feedback (id, email_id, kind, value, account_email, created_at)
                VALUES (?, ?, 'priority_correction', 'red', ?, ?)
                """,
                (
                    f"fb-{email_id}",
                    email_id,
                    ACCOUNT,
                    (now - offset).strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        conn.commit()
    return db_path


def test_dashboard_counts_respect_their_windows(tmp_path: Path) -> None:
    db_path = _seed_windows_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        data = client.get("/api/dashboard", query_string={"account_emails": ACCOUNT}).get_json()

    assert data["emails_today"] == 2  # last 24 h: 10 min and 3 h ago
    assert data["emails_last_hour"] == 1
    assert data["llm_calls_today"] == 2
    assert data["priority"]["red"] == 2
    assert data["corrections_week"] == 1


def test_home_shows_windowed_counts_with_an_honest_label(tmp_path: Path) -> None:
    db_path = _seed_windows_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        body = client.get("/", query_string={"account_emails": ACCOUNT}).get_data(
            as_text=True
        )

    assert 'id="preview-emails-today">2<' in body
    assert 'id="preview-emails-hour">1<' in body
    assert 'id="preview-llm-calls">2<' in body
    # A rolling 24-hour count is not "today".
    assert "Emails today" not in body
    assert "Emails (24 h)" in body


def test_top_sender_profiles_only_count_the_requested_days(tmp_path: Path) -> None:
    db_path = tmp_path / "profiles-window.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_email(conn, sender="recent@example.com", received=now - timedelta(days=1))
        for days in (20, 21, 22):
            _insert_email(
                conn, sender="old@example.com", received=now - timedelta(days=days)
            )
        conn.commit()

    profiles = KnowledgeAnalytics(db_path).top_sender_relationship_profiles(
        account_email=ACCOUNT, days=7, limit=5, now=now, include_raw_senders=True
    )

    assert [profile["sender_email"] for profile in profiles] == ["recent@example.com"]
