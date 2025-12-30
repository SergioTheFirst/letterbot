from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email import message_from_bytes
from email.message import EmailMessage
from email.utils import parseaddr
from pathlib import Path
from typing import Callable, Iterable

from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    PipelineContext,
    configure_pipeline,
    parse_raw_email,
    remember_raw_email,
    store_inbound,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline.processor import MessageProcessor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.start import _build_system_payload, _process_queue
from mailbot_v26.text.mime_utils import decode_mime_header
from mailbot_v26.worker.telegram_sender import DeliveryResult


@dataclass
class DummyState:
    def get_last_uid(self, _login: str) -> int:
        return 0

    def get_last_check_time(self, _login: str) -> None:
        return None

    def update_last_uid(self, _login: str, _uid: int) -> None:
        return None

    def update_check_time(self, _login: str, _timestamp: datetime | None = None) -> None:
        return None

    def set_imap_status(self, _login: str, _status: str, _error: str = "") -> None:
        return None

    def save(self) -> None:
        return None


def build_config(tmp_path: Path, accounts: list[AccountConfig]) -> BotConfig:
    general = GeneralConfig(
        check_interval=1,
        max_email_mb=15,
        max_attachment_mb=1,
        max_zip_uncompressed_mb=80,
        max_extracted_chars=50_000,
        max_extracted_total_chars=120_000,
        admin_chat_id="admin",
    )
    keys = KeysConfig(
        telegram_bot_token="token",
        cf_account_id="cf",
        cf_api_token="cf_token",
    )
    storage = StorageConfig(db_path=tmp_path / "mailbot.sqlite")
    return BotConfig(general=general, accounts=accounts, keys=keys, storage=storage)


def build_raw_email(*, subject: str, body: str, sender: str = "sender@example.com") -> bytes:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = "receiver@example.com"
    message.set_content(body)
    return message.as_bytes()


def cleanup_pipeline_cache(email_ids: Iterable[int]) -> None:
    for email_id in email_ids:
        PIPELINE_CACHE.pop(email_id, None)
        PIPELINE_INBOUND_CACHE.pop(email_id, None)
        PIPELINE_RAW_CACHE.pop(email_id, None)


def run_single_cycle(
    *,
    config: BotConfig,
    accounts_to_poll: list[AccountConfig],
    imap_factory: Callable[[AccountConfig, DummyState, datetime], object],
    processor: MessageProcessor,
    runtime_health: AccountRuntimeHealthManager,
    storage: Storage,
    now: datetime,
    telegram_sender: Callable[[TelegramPayload], DeliveryResult],
) -> None:
    configure_pipeline(config, processor)
    state = DummyState()

    for account in accounts_to_poll:
        if not runtime_health.should_attempt(account.account_id, now):
            continue

        try:
            imap = imap_factory(account, state, now)
            new_messages = imap.fetch_new_messages()
            runtime_health.on_success(account.account_id, now)
        except Exception as exc:
            should_alert, alert_text = runtime_health.on_failure(account.account_id, exc, now)
            if should_alert:
                payload = _build_system_payload(
                    text=alert_text,
                    bot_token=config.keys.telegram_bot_token,
                    chat_id=account.telegram_chat_id,
                    priority="🔴",
                )
                telegram_sender(payload)
            continue

        for uid, raw in new_messages:
            inbound = parse_raw_email(raw, config)
            message_obj = message_from_bytes(raw)
            message_id = message_obj.get("Message-ID") if message_obj else None
            from_header = decode_mime_header(message_obj.get("From", "")) if message_obj else ""
            _, from_email = parseaddr(from_header or inbound.sender)
            received_at = decode_mime_header(message_obj.get("Date", "")) if message_obj else None
            attachments_count = len(inbound.attachments or [])

            email_id = storage.upsert_email(
                account_email=account.login,
                uid=uid,
                message_id=message_id,
                from_email=from_email or None,
                from_name=None,
                subject=inbound.subject,
                received_at=received_at or None,
                attachments_count=attachments_count,
            )
            ctx = PipelineContext(email_id=email_id, account_email=account.login, uid=uid)
            PIPELINE_CACHE[email_id] = ctx
            remember_raw_email(email_id, raw)
            store_inbound(email_id, inbound)
            storage.enqueue_stage(email_id, "PARSE")

    _process_queue(storage, config, processor)


__all__ = [
    "DummyState",
    "build_config",
    "build_raw_email",
    "cleanup_pipeline_cache",
    "run_single_cycle",
]
