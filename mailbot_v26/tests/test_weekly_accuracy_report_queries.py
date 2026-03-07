from __future__ import annotations

from datetime import datetime, timezone
import sqlite3

import pytest

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts_utc: float,
    account_email: str,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_email,
            entity_id=None,
            email_id=None,
            payload=payload,
        )
    )


def test_weekly_accuracy_report_counts_and_rate(tmp_path) -> None:
    db_path = tmp_path / "accuracy.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(3):
        _emit_event(
            emitter,
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={},
        )

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=now.timestamp() + 10 + offset,
            account_email="acc@example.com",
            payload={"old_priority": "🔵", "new_priority": "🔴"},
        )

    _emit_event(
        emitter,
        event_type=EventType.SURPRISE_DETECTED,
        ts_utc=now.timestamp() + 20,
        account_email="acc@example.com",
        payload={},
    )

    report = analytics.weekly_accuracy_report(
        account_email="acc@example.com",
        days=7,
    )
    assert report["emails_received"] == 3
    assert report["priority_corrections"] == 2
    assert report["surprises"] == 1
    assert report["surprise_rate"] == pytest.approx(0.5)
    assert report["accuracy_pct"] == 50


def test_weekly_accuracy_report_skips_rate_without_corrections(tmp_path) -> None:
    db_path = tmp_path / "accuracy.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(2):
        _emit_event(
            emitter,
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=now.timestamp() + offset,
            account_email="acc@example.com",
            payload={},
        )
    _emit_event(
        emitter,
        event_type=EventType.SURPRISE_DETECTED,
        ts_utc=now.timestamp() + 5,
        account_email="acc@example.com",
        payload={},
    )

    report = analytics.weekly_accuracy_report(
        account_email="acc@example.com",
        days=7,
    )
    assert report["priority_corrections"] == 0
    assert "surprise_rate" not in report
    assert "accuracy_pct" not in report


def test_weekly_compact_summary_returns_expected_counts(tmp_path) -> None:
    db_path = tmp_path / "weekly_compact.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, received_at, priority, action_line, body_summary, raw_body_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "acc@example.com",
                "from@example.com",
                "A",
                now.isoformat(),
                "🔴",
                "",
                "",
                "h1",
            ),
        )
        email_id = conn.execute("SELECT id FROM emails ORDER BY id DESC LIMIT 1").fetchone()[0]
        conn.execute(
            """
            INSERT INTO commitments (email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at)
            VALUES (?, 'llm', 'Task', NULL, 'pending', 0.8, ?)
            """,
            (email_id, now.isoformat()),
        )
        conn.commit()

    _emit_event(
        emitter,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=now.timestamp(),
        account_email="acc@example.com",
        payload={},
    )
    _emit_event(
        emitter,
        event_type=EventType.PRIORITY_CORRECTION_RECORDED,
        ts_utc=now.timestamp() + 1,
        account_email="acc@example.com",
        payload={"old_priority": "🔵", "new_priority": "🔴"},
    )

    summary = analytics.weekly_compact_summary(account_email="acc@example.com", days=7)

    assert summary == {
        "emails_total": 1,
        "important": 1,
        "low": 0,
        "corrections": 1,
        "accuracy_pct": 100,
        "open_commitments": 1,
    }
