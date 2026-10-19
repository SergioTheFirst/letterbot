from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import create_app

_RAW_SENDERS = ("alice@example.com", "alice+billing@example.com")


def _seed_emails(db_path: Path) -> None:
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        for sender, subject in zip(_RAW_SENDERS, ("Invoice", "Contract")):
            conn.execute(
                """
                INSERT INTO emails (account_email, from_email, subject, body_summary, priority, received_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("primary@example.com", sender, subject, "summary", "🔴", now, now),
            )
        conn.commit()


def test_api_dashboard_top_contacts_contain_no_raw_sender_address(tmp_path) -> None:
    db_path = tmp_path / "dashboard-privacy.sqlite"
    _seed_emails(db_path)

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    for raw in _RAW_SENDERS:
        assert raw not in body
    contacts = resp.get_json()["top_contacts"]
    assert contacts
    assert contacts[0]["sender_email"] == "a…@example.com"
    assert contacts[0]["emails_count"] == 2
    assert "sender_emails" not in contacts[0]
    assert "sender_identity_key" not in contacts[0]


def test_top_sender_profiles_keep_raw_senders_only_on_request(tmp_path) -> None:
    db_path = tmp_path / "profiles.sqlite"
    _seed_emails(db_path)
    analytics = KnowledgeAnalytics(db_path)

    masked = analytics.top_sender_relationship_profiles(
        account_email="primary@example.com", days=7, limit=5
    )
    raw = analytics.top_sender_relationship_profiles(
        account_email="primary@example.com",
        days=7,
        limit=5,
        include_raw_senders=True,
    )

    assert masked[0]["sender_email"] == "a…@example.com"
    assert raw[0]["sender_email"] == "alice+billing@example.com"
    assert raw[0]["sender_emails"] == sorted(_RAW_SENDERS)
