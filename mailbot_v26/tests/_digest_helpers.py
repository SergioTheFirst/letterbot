"""Shared helpers for the digest scheduler tests (local time, scope, delivery)."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import digest_scheduler
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult

MSK = timezone(timedelta(hours=3))
NEW_YORK_WINTER = timezone(timedelta(hours=-5))
DELIVERED = DeliveryResult(delivered=True, retryable=False)


class RecordingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        self.events.append(("info", event, fields))

    def warning(self, event: str, **fields: object) -> None:
        self.events.append(("warning", event, fields))

    def error(self, event: str, **fields: object) -> None:
        self.events.append(("error", event, fields))

    def named(self, event: str) -> list[dict[str, object]]:
        return [fields for _level, name, fields in self.events if name == event]

    def checked(self, reason: str | None = None) -> list[dict[str, object]]:
        return [
            fields
            for fields in self.named("digest_tick_checked")
            if reason is None or fields.get("reason") == reason
        ]


class ScriptedSender:
    """Telegram stand-in: returns scripted results (DELIVERED once exhausted)."""

    def __init__(self, *results: object) -> None:
        self._results = list(results)
        self.payloads: list[object] = []

    def __call__(self, payload) -> DeliveryResult:
        self.payloads.append(payload)
        result = self._results.pop(0) if self._results else DELIVERED
        if isinstance(result, BaseException):
            raise result
        return result  # type: ignore[return-value]

    @property
    def chats(self) -> list[str]:
        return [str(payload.metadata.get("chat_id")) for payload in self.payloads]


def account(login: str, chat_id: str = "chat") -> AccountConfig:
    return AccountConfig(
        account_id=login.split("@", 1)[0].lower() or "acc",
        login=login,
        password="pass",
        host="",
        port=993,
        use_ssl=True,
        telegram_chat_id=chat_id,
    )


def bot_config(tmp_path: Path, accounts: list[AccountConfig]) -> BotConfig:
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
        accounts=accounts,
        keys=KeysConfig(telegram_bot_token="token", cf_account_id="", cf_api_token=""),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def build_storage(tmp_path: Path) -> digest_scheduler.DigestStorage:
    db_path = tmp_path / "knowledge.sqlite"
    return digest_scheduler.DigestStorage(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        event_emitter=EventEmitter(tmp_path / "events.sqlite"),
        contract_event_emitter=ContractEventEmitter(db_path),
    )


def write_config(
    tmp_path: Path,
    monkeypatch,
    *,
    daily: bool,
    weekly: bool,
    extra: str = "",
    weekly_weekday: str = "mon",
    weekly_hour: int = 9,
) -> Path:
    config_path = tmp_path / "config.ini"
    config_path.write_text(
        "[features]\n"
        f"enable_daily_digest = {str(daily).lower()}\n"
        f"enable_weekly_digest = {str(weekly).lower()}\n"
        f"{extra}"
        "\n"
        "[daily_digest]\n"
        "hour = 9\n"
        "minute = 0\n"
        "\n"
        "[weekly_digest]\n"
        f"weekday = {weekly_weekday}\n"
        f"hour = {weekly_hour}\n"
        "minute = 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)
    return config_path


def emit(storage: digest_scheduler.DigestStorage, event: EventV1) -> None:
    assert storage.contract_event_emitter is not None
    storage.contract_event_emitter.emit(event)


def seed_deferred(
    storage: digest_scheduler.DigestStorage,
    account_id: str,
    *,
    from_email: str = "sender@example.com",
) -> int:
    """One deferred email (daily + weekly content) stored under account_id."""
    email_id = storage.knowledge_db.save_email(
        account_email=account_id,
        from_email=from_email,
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        priority="🔵",
        action_line="Check",
        body_summary="",
        raw_body="",
        attachment_summaries=[("report.pdf", "summary")],
        deferred_for_digest=True,
    )
    assert email_id is not None
    now_ts = datetime.now(timezone.utc).timestamp()
    for event_type, payload in (
        (EventType.EMAIL_RECEIVED, {"attachments_count": 1}),
        (
            EventType.ATTENTION_DEFERRED_FOR_DIGEST,
            {"reason": "test", "attachments_only": True, "attachments_count": 1},
        ),
    ):
        emit(
            storage,
            EventV1(
                event_type=event_type,
                ts_utc=now_ts,
                account_id=account_id,
                entity_id=None,
                email_id=email_id,
                payload=payload,
            ),
        )
    return email_id


def emit_score_pair(
    storage: digest_scheduler.DigestStorage,
    *,
    account_id: str,
    entity_id: str,
    event_type: EventType,
    field: str,
    before: float,
    after: float,
) -> None:
    """Two recent score events for one entity (a non-zero delta)."""
    now = datetime.now(timezone.utc)
    for offset_hours, value in ((6, before), (1, after)):
        emit(
            storage,
            EventV1(
                event_type=event_type,
                ts_utc=(now - timedelta(hours=offset_hours)).timestamp(),
                account_id=account_id,
                entity_id=entity_id,
                email_id=None,
                payload={field: value, "model_version": "v2"},
            ),
        )


def seed_interaction(
    storage: digest_scheduler.DigestStorage, *, account_id: str, entity_id: str
) -> None:
    """An email of account_id from entity_id, as the context layer records it."""
    email_id = storage.knowledge_db.save_email(
        account_email=account_id,
        from_email=f"{entity_id}@example.com",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        priority="🔵",
        action_line="Check",
        body_summary="",
        raw_body="",
        attachment_summaries=[],
    )
    with closing(sqlite3.connect(storage.knowledge_db.path)) as conn, conn:
        conn.execute(
            "INSERT INTO interaction_events (id, entity_id, event_type, event_time, "
            "metadata) VALUES (?, ?, 'email_received', ?, ?)",
            (
                f"{entity_id}-{email_id}",
                entity_id,
                datetime.now(timezone.utc).isoformat(),
                json.dumps({"email_id": email_id}),
            ),
        )


def tick(
    config: BotConfig,
    storage: digest_scheduler.DigestStorage,
    sender,
    now: datetime,
    logger: RecordingLogger | None = None,
) -> RecordingLogger:
    used = logger or RecordingLogger()
    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=sender,
        logger=used,
    )
    return used


def override_value(storage: digest_scheduler.DigestStorage, key: str) -> str | None:
    with closing(sqlite3.connect(storage.knowledge_db.path)) as conn:
        row = conn.execute(
            "SELECT value FROM runtime_overrides WHERE key = ?", (key,)
        ).fetchone()
    return None if row is None else row[0]


def daily_sent_key(login: str) -> str:
    return digest_scheduler._digest_marker_key(digest_scheduler._DAILY_SENT_KEY, login)


def weekly_sent_key(login: str) -> str:
    return digest_scheduler._digest_marker_key(digest_scheduler._WEEKLY_SENT_KEY, login)
