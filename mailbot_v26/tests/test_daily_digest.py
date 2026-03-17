from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mailbot_v26.config_loader import AccountScope
from mailbot_v26.pipeline import daily_digest
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _seed_deferred_email(db: KnowledgeDB, emitter: ContractEventEmitter) -> None:
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
    now_ts = datetime.now(timezone.utc).timestamp()
    emitter.emit(
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
    emitter.emit(
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


def _seed_attention_emails(db: KnowledgeDB, emitter: ContractEventEmitter) -> None:
    for idx in range(5):
        email_id = db.save_email(
            account_email="account@example.com",
            from_email="client@example.com" if idx < 3 else "vendor@example.com",
            subject=f"Subject {idx}",
            received_at=datetime.now(timezone.utc).isoformat(),
            priority="🔵",
            action_line="Проверить",
            body_summary=f"Текст письма {idx} для метрик внимания",
            raw_body="",
            attachment_summaries=[],
        )
        assert email_id is not None
        emitter.emit(
            EventV1(
                event_type=EventType.EMAIL_RECEIVED,
                ts_utc=datetime.now(timezone.utc).timestamp(),
                account_id="account@example.com",
                entity_id=None,
                email_id=email_id,
                payload={
                    "from_email": (
                        "client@example.com" if idx < 3 else "vendor@example.com"
                    ),
                    "subject": f"Subject {idx}",
                    "body_summary": f"Текст письма {idx} для метрик внимания",
                    "attachments_count": 0,
                },
            )
        )


def _emit_trust_event(
    emitter: ContractEventEmitter,
    *,
    ts_utc: float,
    entity_id: str,
    score: float,
    model_version: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.TRUST_SCORE_UPDATED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=entity_id,
            email_id=None,
            payload={
                "trust_score": score,
                "sample_size": 3,
                "data_window_days": 30,
                "model_version": model_version,
            },
        )
    )


def _emit_received(
    emitter: ContractEventEmitter,
    *,
    ts: datetime,
    account_id: str,
    email_id: int,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=ts.timestamp(),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={"from_email": "sender@example.com"},
        )
    )


def _emit_correction(
    emitter: ContractEventEmitter,
    *,
    ts: datetime,
    account_id: str,
    email_id: int,
    new_priority: str,
) -> None:
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=ts.timestamp(),
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload={
                "old_priority": "🟡",
                "new_priority": new_priority,
                "engine": "priority_v2",
            },
        )
    )


def test_daily_digest_sent_once_per_day(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _seed_deferred_email(db, contract_emitter)

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
        email_id=101,
        contract_event_emitter=contract_emitter,
    )

    daily_digest.maybe_send_daily_digest(
        knowledge_db=db,
        analytics=analytics,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=102,
        contract_event_emitter=contract_emitter,
    )

    assert len(sent) == 1
    last_sent = db.get_last_digest_sent_at(account_email="account@example.com")
    assert last_sent is not None


def test_daily_digest_not_sent_without_content(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)

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
        email_id=201,
        contract_event_emitter=contract_emitter,
    )

    assert sent == []
    assert db.get_last_digest_sent_at(account_email="account@example.com") is None


def test_daily_digest_sent_with_deferred_items(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _seed_deferred_email(db, contract_emitter)

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
        email_id=301,
        contract_event_emitter=contract_emitter,
    )

    assert len(sent) == 1
    payload = sent[0]["payload"]
    assert "Отложено писем" in payload.html_text
    assert "Powered by LetterBot.ru" in payload.html_text


def test_daily_digest_attention_block(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _seed_attention_emails(db, contract_emitter)

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
        email_id=401,
        include_attention_economics=True,
        contract_event_emitter=contract_emitter,
    )

    assert len(sent) == 1
    payload = sent[0]["payload"]
    assert "Куда ушло внимание" in payload.html_text
    assert "Лучшие контрагенты" in payload.html_text


def test_digest_or_preview_uses_v2_when_available(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now = datetime.now(timezone.utc)

    _emit_trust_event(
        contract_emitter,
        ts_utc=(now - timedelta(days=10)).timestamp(),
        entity_id="entity-1",
        score=0.4,
        model_version="v1",
    )
    _emit_trust_event(
        contract_emitter,
        ts_utc=(now - timedelta(days=5)).timestamp(),
        entity_id="entity-1",
        score=0.6,
        model_version="v1",
    )
    _emit_trust_event(
        contract_emitter,
        ts_utc=(now - timedelta(days=3)).timestamp(),
        entity_id="entity-1",
        score=0.7,
        model_version="v2",
    )
    _emit_trust_event(
        contract_emitter,
        ts_utc=(now - timedelta(days=1)).timestamp(),
        entity_id="entity-1",
        score=0.8,
        model_version="v2",
    )

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email="account@example.com",
        now=now,
    )

    assert data.trust_delta == pytest.approx(0.1)


def test_daily_digest_quality_metrics_aggregates_account_scope(
    monkeypatch, tmp_path
) -> None:
    db_path = tmp_path / "daily_quality.sqlite"
    KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    anchor = datetime(2024, 1, 7, tzinfo=timezone.utc)

    _emit_received(
        contract_emitter,
        ts=anchor,
        account_id="account@example.com",
        email_id=1,
    )
    _emit_received(
        contract_emitter,
        ts=anchor,
        account_id="alt@example.com",
        email_id=2,
    )
    _emit_correction(
        contract_emitter,
        ts=anchor,
        account_id="account@example.com",
        email_id=1,
        new_priority="🔴",
    )
    _emit_correction(
        contract_emitter,
        ts=anchor,
        account_id="alt@example.com",
        email_id=2,
        new_priority="🟡",
    )

    monkeypatch.setattr(
        daily_digest,
        "resolve_account_scope",
        lambda *_args, **_kwargs: AccountScope(
            chat_id="chat",
            account_emails=["account@example.com", "alt@example.com"],
        ),
    )

    data = daily_digest._collect_digest_data(
        analytics=analytics,
        account_email="account@example.com",
        include_quality_metrics=True,
        now=anchor,
        contract_event_emitter=contract_emitter,
    )

    assert data.quality_metrics is not None
    assert data.quality_metrics.corrections_total == 2
    assert data.quality_metrics.emails_received == 2
    assert data.quality_metrics.correction_rate == 1.0
