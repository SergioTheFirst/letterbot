from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.pipeline import digest_scheduler
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.worker.telegram_sender import DeliveryResult


class DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.events.append(("info", event, fields))

    def warning(self, event: str, **fields: object) -> None:
        self.events.append(("warning", event, fields))

    def error(self, event: str, **fields: object) -> None:
        self.events.append(("error", event, fields))


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


def _write_config(path: Path, *, daily_enabled: bool = True, weekly_enabled: bool = False) -> None:
    path.write_text(
        """
[features]
"""
        + f"enable_daily_digest = {str(daily_enabled).lower()}\n"
        + f"enable_weekly_digest = {str(weekly_enabled).lower()}\n"
        + """

[daily_digest]
hour = 9
minute = 0

[weekly_digest]
weekday = mon
hour = 9
minute = 0
""",
        encoding="utf-8",
    )


def _build_config(tmp_path: Path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50000,
            max_extracted_total_chars=120000,
            admin_chat_id="",
        ),
        accounts=[
            AccountConfig(
                account_id="acc",
                login="account@example.com",
                password="pass",
                host="",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            )
        ],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="",
            cf_api_token="",
        ),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def _build_storage(tmp_path: Path) -> digest_scheduler.DigestStorage:
    db_path = tmp_path / "knowledge.sqlite"
    return digest_scheduler.DigestStorage(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        event_emitter=EventEmitter(tmp_path / "events.sqlite"),
        contract_event_emitter=ContractEventEmitter(db_path),
    )


def test_scheduler_daily_due_no_mail(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=True, weekly_enabled=False)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    config = _build_config(tmp_path)
    storage = _build_storage(tmp_path)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)

    sent: list[dict[str, object]] = []

    def _sender(payload) -> DeliveryResult:
        sent.append({"payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc)
    logger = DummyLogger()

    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    assert len(sent) == 1
    assert storage.knowledge_db.get_last_digest_sent_at(
        account_email="account@example.com"
    ) == now


def test_scheduler_idempotency(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=True, weekly_enabled=False)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    config = _build_config(tmp_path)
    storage = _build_storage(tmp_path)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)

    sent: list[dict[str, object]] = []

    def _sender(payload) -> DeliveryResult:
        sent.append({"payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2025, 1, 6, 9, 2, tzinfo=timezone.utc)
    logger = DummyLogger()

    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )
    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    assert len(sent) == 1


def test_scheduler_weekly_iso_logic(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=False, weekly_enabled=True)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    config = _build_config(tmp_path)
    storage = _build_storage(tmp_path)

    sent: list[dict[str, object]] = []

    def _sender(payload) -> DeliveryResult:
        sent.append({"payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2025, 12, 29, 9, 0, tzinfo=timezone.utc)
    logger = DummyLogger()

    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    assert len(sent) == 1
    assert storage.knowledge_db.get_last_weekly_digest_key(
        account_email="account@example.com"
    ) == "2026-W01"


def test_digest_override_disables_dispatch(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=True, weekly_enabled=False)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    config = _build_config(tmp_path)
    storage = _build_storage(tmp_path)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)
    RuntimeOverrideStore(storage.knowledge_db.path).set_digest_enabled(False)

    sent: list[dict[str, object]] = []

    def _sender(payload) -> DeliveryResult:
        sent.append({"payload": payload})
        return DeliveryResult(delivered=True, retryable=False)

    now = datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc)
    logger = DummyLogger()

    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    assert sent == []
    assert any(
        event == "digest_tick_checked" and fields.get("reason") == "override_disabled"
        for _, event, fields in logger.events
    )


def test_scheduler_error_isolation(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily_enabled=True, weekly_enabled=False)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    config = _build_config(tmp_path)
    storage = _build_storage(tmp_path)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)

    def _sender(_payload) -> DeliveryResult:
        raise RuntimeError("telegram down")

    now = datetime(2025, 1, 6, 9, 3, tzinfo=timezone.utc)
    logger = DummyLogger()

    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=_sender,
        logger=logger,
    )

    error_events = [event for level, event, _ in logger.events if level == "error"]
    assert "digest_failed" in error_events
