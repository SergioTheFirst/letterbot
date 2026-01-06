from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26 import imap_client
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.pipeline import processor
from mailbot_v26.bot_core.pipeline import parse_raw_email
from mailbot_v26.state_manager import StateManager
from mailbot_v26.worker.telegram_sender import DeliveryResult


class _OversizeIMAPClient:
    def __init__(self, message_size: int, raw_header: bytes) -> None:
        self.message_size = message_size
        self.raw_header = raw_header

    def login(self, login: str, password: str) -> None:
        return None

    def select_folder(self, folder: str) -> None:
        return None

    def search(self, criteria):  # type: ignore[override]
        return [101]

    def fetch(self, uids, fields):  # type: ignore[override]
        uid = uids[0]
        if "RFC822.SIZE" in fields:
            return {
                uid: {
                    b"RFC822.SIZE": self.message_size,
                    b"INTERNALDATE": datetime(2024, 1, 2, 12, 0, 0),
                }
            }
        if "BODY.PEEK[HEADER]" in fields:
            return {uid: {b"BODY[HEADER]": self.raw_header}}
        return {uid: {b"RFC822": b""}}

    def logout(self) -> None:
        return None


def _build_config(tmp_path: Path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=10,
            max_email_mb=15,
            max_attachment_mb=10,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="",
        ),
        accounts=[],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="cf",
            cf_api_token="api",
        ),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def test_oversize_email_warns_in_telegram(monkeypatch, tmp_path: Path) -> None:
    raw_header = b"From: sender@example.com\r\nSubject: Oversize\r\n\r\n"
    fake_client = _OversizeIMAPClient(
        message_size=2 * 1024 * 1024,
        raw_header=raw_header,
    )
    monkeypatch.setattr(processor, "enqueue_tg", lambda **_: DeliveryResult(delivered=True, retryable=False))
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary=kwargs["body_text"],
        attachment_summaries=[],
        llm_provider="gigachat",
    ))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_DAILY_DIGEST=False,
            ENABLE_COMMITMENT_TRACKER=False,
        ),
    )
    monkeypatch.setattr(processor.shadow_priority_engine, "compute", lambda **_: ("🔵", None))
    monkeypatch.setattr(processor.shadow_action_engine, "compute", lambda **_: [])
    monkeypatch.setattr(processor, "evaluate_signal_quality", lambda *_args, **_kwargs: SimpleNamespace(
        entropy=1.0,
        printable_ratio=1.0,
        quality_score=1.0,
        is_usable=True,
        reason="ok",
    ))
    monkeypatch.setattr(
        processor,
        "apply_attention_gate",
        lambda *_args, **_kwargs: SimpleNamespace(deferred=False, reason="default_send"),
    )
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(
        path=":memory:",
        save_email=lambda **kwargs: None,
        fetch_pending_commitments_by_sender=lambda **kwargs: [],
        update_commitment_statuses=lambda **kwargs: True,
        save_commitments=lambda **kwargs: True,
        upsert_entity_signal=lambda **kwargs: None,
    ))
    monkeypatch.setattr(processor, "analytics", SimpleNamespace(
        sender_stats=lambda: [],
        priority_escalations=lambda **_: [],
        commitment_stats_by_sender=lambda **_: {
            "total_commitments": 0,
            "fulfilled_count": 0,
            "expired_count": 0,
            "unknown_count": 0,
        },
    ))
    monkeypatch.setattr(processor.context_store, "resolve_sender_entity", lambda **_: None)
    monkeypatch.setattr(processor.context_store, "record_interaction_event", lambda **_: None)
    monkeypatch.setattr(processor.context_store, "recompute_email_frequency", lambda **_: (0.0, 0))
    monkeypatch.setattr(processor, "event_emitter", SimpleNamespace(emit=lambda **_: None))
    monkeypatch.setattr(processor, "decision_trace_writer", SimpleNamespace(write=lambda **_: None))

    monkeypatch.setattr(imap_client, "IMAPClient", lambda *args, **kwargs: fake_client)

    account = AccountConfig(
        account_id="test",
        login="user@example.com",
        password="secret",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    state = StateManager(tmp_path / "state.json")
    imap = ResilientIMAP(account, state, start_time=datetime(2024, 1, 2, 10, 0, 0), max_email_mb=1)
    messages = imap.fetch_new_messages()
    assert len(messages) == 1

    raw_message = messages[0][1]
    inbound = parse_raw_email(raw_message, _build_config(tmp_path))
    assert "Письмо слишком большое" in inbound.body

    captured: dict[str, str] = {}

    def _capture_payload(*, email_id: int, payload):
        captured["html"] = payload.html_text
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _capture_payload)

    processor.process_message(
        account_email="user@example.com",
        message_id=55,
        from_email="sender@example.com",
        subject=inbound.subject,
        received_at=datetime(2024, 1, 2, 12, 0, 0),
        body_text=inbound.body,
        attachments=[],
        telegram_chat_id="chat",
    )

    assert "Письмо слишком большое" in captured["html"]
