from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_emails(db_path: Path) -> None:
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO emails (
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                priority_reason,
                action_line,
                body_summary,
                raw_body_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "a1@example.com",
                    "alpha@example.com",
                    "Escalated thread",
                    datetime(2024, 6, 1, 10, 0).isoformat(),
                    "🔴",
                    "SLA breach",
                    "Do now",
                    "Red summary",
                    "h1",
                ),
                (
                    "a1@example.com",
                    "alpha@example.com",
                    "Normal follow-up",
                    datetime(2024, 6, 2, 11, 0).isoformat(),
                    "🟡",
                    None,
                    "Do soon",
                    "Yellow summary",
                    "h2",
                ),
                (
                    "a2@example.com",
                    "alpha@example.com",
                    "Info only",
                    datetime(2024, 6, 3, 12, 0).isoformat(),
                    "🔵",
                    "",
                    "FYI",
                    "Blue summary",
                    "h3",
                ),
                (
                    "a2@example.com",
                    "bravo@example.com",
                    "Second escalation",
                    datetime(2024, 6, 4, 13, 0).isoformat(),
                    "🟡",
                    "Awaiting approval",
                    "Check",
                    "Escalated yellow",
                    "h4",
                ),
            ],
        )
        conn.commit()


def test_views_and_read_only_analytics(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    _seed_emails(db_path)

    with sqlite3.connect(db_path) as conn:
        view_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='view';"
            ).fetchall()
        }

    assert {"v_sender_stats", "v_account_stats", "v_priority_escalations"} <= view_names

    analytics = KnowledgeAnalytics(db_path)

    sender_stats = analytics.sender_stats()
    account_stats = analytics.account_stats()
    escalations = analytics.priority_escalations()

    alpha = next(row for row in sender_stats if row["sender_email"] == "alpha@example.com")
    assert alpha["emails_total"] == 3
    assert alpha["account_count"] == 2
    assert alpha["red_count"] == 1
    assert alpha["yellow_count"] == 1
    assert alpha["blue_count"] == 1
    assert alpha["escalations"] == 1

    bravo = next(row for row in sender_stats if row["sender_email"] == "bravo@example.com")
    assert bravo["emails_total"] == 1
    assert bravo["escalations"] == 1

    account_a1 = next(row for row in account_stats if row["account_email"] == "a1@example.com")
    assert account_a1["emails_total"] == 2
    assert account_a1["sender_count"] == 1
    assert account_a1["red_count"] == 1
    assert account_a1["yellow_count"] == 1
    assert account_a1["escalations"] == 1

    account_a2 = next(row for row in account_stats if row["account_email"] == "a2@example.com")
    assert account_a2["emails_total"] == 2
    assert account_a2["sender_count"] == 2
    assert account_a2["blue_count"] == 1
    assert account_a2["escalations"] == 1

    assert len(escalations) == 2
    assert escalations[0]["from_email"] == "bravo@example.com"
    assert escalations[1]["from_email"] == "alpha@example.com"

    with analytics._connect_readonly() as conn:  # type: ignore[attr-defined]
        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                """
                INSERT INTO emails (
                    account_email,
                    from_email,
                    subject,
                    received_at,
                    priority,
                    action_line,
                    body_summary,
                    raw_body_hash
                )
                VALUES ('', '', '', '', '', '', '', '')
                """
            )


def test_cockpit_contact_methods_return_empty_on_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)

    assert analytics.cockpit_top_senders(["a@example.com"]) == []
    assert analytics.cockpit_silent_contacts(["a@example.com"]) == []
    assert analytics.cockpit_stalled_threads(["a@example.com"]) == []
    assert analytics.count_all_time_corrections(account_emails=["a@example.com"]) == 0
