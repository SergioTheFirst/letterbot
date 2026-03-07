from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tasks.shadow_actions import ShadowActionEngine


def _seed(db_path: Path) -> None:
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
                    "ops@example.com",
                    "alpha@example.com",
                    "Escalated invoice",
                    datetime(2024, 6, 1, 9, 0).isoformat(),
                    "🔴",
                    "SLA breach",
                    "Do now",
                    "Urgent payment",
                    "h1",
                ),
                (
                    "ops@example.com",
                    "alpha@example.com",
                    "Second escalation",
                    datetime(2024, 6, 2, 10, 0).isoformat(),
                    "🔴",
                    "Awaiting approval",
                    "Approve",
                    "Follow-up",
                    "h2",
                ),
                (
                    "ops@example.com",
                    "alpha@example.com",
                    "Another red",
                    datetime(2024, 6, 3, 11, 0).isoformat(),
                    "🔴",
                    None,
                    "Check",
                    "Red summary",
                    "h3",
                ),
                (
                    "ops@example.com",
                    "bravo@example.com",
                    "Latest approval",
                    datetime(2024, 6, 5, 12, 0).isoformat(),
                    "🟡",
                    "Pending manager approval",
                    "Review",
                    "Yellow summary",
                    "h4",
                ),
            ],
        )
        conn.commit()


def test_shadow_action_engine_surfaces_hypothetical_tasks(tmp_path: Path) -> None:
    db_path = tmp_path / "database.sqlite"
    _seed(db_path)

    analytics = KnowledgeAnalytics(db_path)
    engine = ShadowActionEngine(analytics)

    tasks = engine.compute(
        account_email="ops@example.com",
        from_email="alpha@example.com",
    )

    assert any("🔴" in reason for _, reason in tasks)
    assert any("эскалации" in reason for _, reason in tasks)
    assert any(task.startswith("Согласовать план") for task, _ in tasks)
    assert any("Latest approval" in task for task, _ in tasks)


def test_shadow_action_engine_handles_missing_context(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"
    KnowledgeDB(db_path)  # initialize schema

    analytics = KnowledgeAnalytics(db_path)
    engine = ShadowActionEngine(analytics)

    tasks = engine.compute(account_email="", from_email="")
    assert tasks == []
