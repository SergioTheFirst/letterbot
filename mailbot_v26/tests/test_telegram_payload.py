from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import TelegramSendResult


def test_telegram_payload_unchanged(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Позвонить клиенту",
        body_summary="Краткое описание письма.",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )

    captured: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        captured["email_id"] = email_id
        captured["payload"] = payload
        return TelegramSendResult(success=True)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=10,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = captured["payload"]
    assert isinstance(payload, TelegramPayload)
    assert payload.metadata["subject"] == "Subject"
    assert payload.metadata["sender"] == "sender@example.com"
    assert payload.metadata["extracted_text"] == "Body"


def test_telegram_payload_unchanged_with_gigachat_provider(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Позвонить клиенту",
        body_summary="Краткое описание письма.",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )

    captured: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        captured["email_id"] = email_id
        captured["payload"] = payload
        return TelegramSendResult(success=True)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=10,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = captured["payload"]
    assert isinstance(payload, TelegramPayload)
    assert payload.metadata["subject"] == "Subject"
    assert payload.metadata["sender"] == "sender@example.com"
    assert payload.metadata["extracted_text"] == "Body"
