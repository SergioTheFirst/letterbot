from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tools.backfill_events import run_backfill
from mailbot_v26.worker.telegram_sender import DeliveryResult


def test_daily_digest_reads_events_not_emails(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)

    email_id = db.save_email(
        account_email="account@example.com",
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        priority="🔵",
        action_line="Проверить",
        body_summary="",
        raw_body="",
        attachment_summaries=[("report.pdf", "summary")],
        deferred_for_digest=True,
    )
    assert email_id is not None

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(daily_digest, "enqueue_tg", _enqueue_tg)

    daily_digest.maybe_send_daily_digest(
        knowledge_db=db,
        analytics=analytics,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=1,
        contract_event_emitter=contract_emitter,
    )

    assert sent == []

    now_ts = datetime.now(timezone.utc).timestamp()
    contract_emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=now_ts,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={
                "from_email": "sender@example.com",
                "subject": "Subject",
                "body_summary": "",
                "attachments_count": 1,
            },
        )
    )
    contract_emitter.emit(
        EventV1(
            event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
            ts_utc=now_ts,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={
                "reason": "test",
                "attachments_only": True,
                "attachments_count": 1,
            },
        )
    )

    daily_digest.maybe_send_daily_digest(
        knowledge_db=db,
        analytics=analytics,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=2,
        contract_event_emitter=contract_emitter,
    )

    assert len(sent) == 1


def test_analytics_from_events(tmp_path) -> None:
    db_path = tmp_path / "analytics.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)

    now = datetime.now(timezone.utc)
    for idx in range(2):
        contract_emitter.emit(
            EventV1(
                event_type=EventType.EMAIL_RECEIVED,
                ts_utc=now.timestamp(),
                account_id="account@example.com",
                entity_id=None,
                email_id=100 + idx,
                payload={
                    "from_email": "sender@example.com",
                    "subject": f"Subject {idx}",
                    "body_summary": "summary text",
                    "attachments_count": 0,
                },
            )
        )
    contract_emitter.emit(
        EventV1(
            event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            entity_id=None,
            email_id=100,
            payload={
                "reason": "test",
                "attachments_only": False,
                "attachments_count": 0,
            },
        )
    )
    contract_emitter.emit(
        EventV1(
            event_type=EventType.COMMITMENT_CREATED,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            entity_id=None,
            email_id=100,
            payload={"commitment_text": "send report"},
        )
    )
    contract_emitter.emit(
        EventV1(
            event_type=EventType.COMMITMENT_STATUS_CHANGED,
            ts_utc=now.timestamp(),
            account_id="account@example.com",
            entity_id=None,
            email_id=100,
            payload={"new_status": "fulfilled"},
        )
    )

    volume = analytics.weekly_email_volume(account_email="account@example.com", days=7)
    assert volume == {"total": 2, "deferred": 1}

    deferred = analytics.deferred_digest_counts(account_email="account@example.com")
    assert deferred["total"] == 1

    commitments = analytics.weekly_commitment_counts(account_email="account@example.com", days=7)
    assert commitments["created"] == 1
    assert commitments["fulfilled"] == 1


def test_backfill_idempotent(tmp_path) -> None:
    db_path = tmp_path / "backfill.sqlite"
    db = KnowledgeDB(db_path)
    email_id = db.save_email(
        account_email="account@example.com",
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        priority="🔵",
        action_line="Проверить",
        body_summary="Summary",
        raw_body="",
        attachment_summaries=[],
        deferred_for_digest=True,
    )
    assert email_id is not None

    run_backfill(db_path, force=True)
    with sqlite3.connect(db_path) as conn:
        first_count = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]

    run_backfill(db_path, force=True)
    with sqlite3.connect(db_path) as conn:
        second_count = conn.execute("SELECT COUNT(*) FROM events_v1").fetchone()[0]

    assert first_count == second_count
