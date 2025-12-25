from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.pipeline import daily_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _seed_deferred_email(db: KnowledgeDB) -> None:
    db.save_email(
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


def test_daily_digest_sent_once_per_day(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    _seed_deferred_email(db)

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
    )

    daily_digest.maybe_send_daily_digest(
        knowledge_db=db,
        analytics=analytics,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=102,
    )

    assert len(sent) == 1
    last_sent = db.get_last_digest_sent_at(account_email="account@example.com")
    assert last_sent is not None


def test_daily_digest_not_sent_without_content(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)

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
    )

    assert sent == []
    assert db.get_last_digest_sent_at(account_email="account@example.com") is None


def test_daily_digest_sent_with_deferred_items(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    _seed_deferred_email(db)

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
    )

    assert len(sent) == 1
    payload = sent[0]["payload"]
    assert "Отложено писем" in payload.html_text
