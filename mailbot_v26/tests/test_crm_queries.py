from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.knowledge_query import KnowledgeQuery


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
                shadow_priority,
                action_line,
                body_summary,
                raw_body_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "account@example.com",
                    "alpha@example.com",
                    "Escalated red",
                    datetime(2024, 7, 1, 9, 0).isoformat(),
                    "🔴",
                    "🔴",
                    "Do now",
                    "Body",
                    "h1",
                ),
                (
                    "account@example.com",
                    "alpha@example.com",
                    "Yellow follow-up",
                    datetime(2024, 7, 2, 10, 0).isoformat(),
                    "🟡",
                    "🔴",
                    "Check later",
                    "Body",
                    "h2",
                ),
                (
                    "account@example.com",
                    "beta@example.com",
                    "Info only",
                    datetime(2024, 7, 3, 11, 0).isoformat(),
                    "🔵",
                    "🟡",
                    "FYI",
                    "Body",
                    "h3",
                ),
                (
                    "account@example.com",
                    "alpha@example.com",
                    "Another yellow",
                    datetime(2024, 7, 4, 12, 0).isoformat(),
                    "🟡",
                    "🟡",
                    "Read",
                    "Body",
                    "h4",
                ),
            ],
        )
        conn.commit()


def test_top_senders_and_priority_distribution(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    _seed_emails(db_path)
    query = KnowledgeQuery(db_path)

    senders = query.top_senders(limit=2)
    assert len(senders) == 2

    alpha = next(row for row in senders if row["from_email"] == "alpha@example.com")
    assert alpha["total_emails"] == 3
    assert alpha["red_count"] == 1
    assert alpha["yellow_count"] == 2
    assert alpha["blue_count"] == 0

    beta = next(row for row in senders if row["from_email"] == "beta@example.com")
    assert beta["total_emails"] == 1
    assert beta["yellow_count"] == 0

    distribution = query.priority_distribution()
    assert distribution == {"🔴": 1, "🟡": 2, "🔵": 1}


def test_shadow_vs_llm_stats_handles_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"
    KnowledgeDB(db_path)
    query = KnowledgeQuery(db_path)

    stats = query.shadow_vs_llm_stats()
    assert stats == {"total": 0, "shadow_diff_pct": 0.0, "shadow_higher_pct": 0.0}


def test_queries_do_not_write(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    _seed_emails(db_path)
    executed: list[str] = []

    original_connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        conn.set_trace_callback(executed.append)
        return conn

    monkeypatch.setattr("mailbot_v26.storage.knowledge_query.sqlite3.connect", traced_connect)

    query = KnowledgeQuery(db_path)
    query.top_senders()
    query.priority_distribution()
    query.shadow_vs_llm_stats()
    query.recent_actions()

    forbidden = ("INSERT", "UPDATE", "DELETE", "REPLACE", "ALTER", "DROP")
    assert all(not stmt.lstrip().upper().startswith(forbidden) for stmt in executed)
