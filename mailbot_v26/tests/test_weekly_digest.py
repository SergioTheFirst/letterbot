from __future__ import annotations

from datetime import datetime, timezone

from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _due_time() -> datetime:
    return datetime(2025, 1, 6, 9, 0, tzinfo=timezone.utc)


def _patch_due_config(monkeypatch) -> None:
    monkeypatch.setattr(
        weekly_digest,
        "_load_weekly_digest_config",
        lambda: weekly_digest.WeeklyDigestConfig(weekday=0, hour=9, minute=0),
    )


def test_weekly_digest_sent_once_per_week(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=100,
        now=_due_time(),
    )
    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=101,
        now=_due_time(),
    )

    assert len(sent) == 1
    assert db.get_last_weekly_digest_key(account_email="account@example.com") == "2025-W02"


def test_weekly_digest_empty_content_is_deterministic(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "weekly.sqlite"
    db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    emitter = EventEmitter(tmp_path / "events.sqlite")

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> DeliveryResult:
        sent.append({"email_id": email_id, "payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(weekly_digest, "enqueue_tg", _enqueue_tg)
    _patch_due_config(monkeypatch)

    weekly_digest.maybe_send_weekly_digest(
        knowledge_db=db,
        analytics=analytics,
        event_emitter=emitter,
        account_email="account@example.com",
        telegram_chat_id="chat",
        email_id=200,
        now=_due_time(),
    )

    assert len(sent) == 1
    html_text = sent[0]["payload"].html_text
    assert "Объём: всего 0, в дайджест 0" in html_text
    assert "Просроченные (топ-5): нет" in html_text
    assert "Trust score: недостаточно истории" in html_text


def test_weekly_digest_flag_disabled_in_config(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        """
[features]
enable_weekly_digest = false
""".strip(),
        encoding="utf-8",
    )
    flags = FeatureFlags(base_dir=config_dir)
    assert flags.ENABLE_WEEKLY_DIGEST is False
