from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import DeliveryResult


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
        return DeliveryResult(delivered=True, retryable=False)

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
        telegram_bot_token="test:token",
    )

    payload = captured["payload"]
    assert isinstance(payload, TelegramPayload)
    assert payload.metadata["subject"] == "Subject"
    assert payload.metadata["sender"] == "sender@example.com"
    assert payload.metadata["extracted_text"] == "Body"
    assert payload.metadata["bot_token"] == "test:token"


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
        return DeliveryResult(delivered=True, retryable=False)

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
        telegram_bot_token="test:token",
    )

    payload = captured["payload"]
    assert isinstance(payload, TelegramPayload)
    assert payload.metadata["subject"] == "Subject"
    assert payload.metadata["sender"] == "sender@example.com"
    assert payload.metadata["extracted_text"] == "Body"


def test_premium_clarity_single_message(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Ответить клиенту",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
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
            ENABLE_PREVIEW_ACTIONS=True,
            ENABLE_PREMIUM_CLARITY_V1=True,
        ),
    )

    captured: list[TelegramPayload] = []

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        captured.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    preview_called = {"called": False}

    def _preview(**_kwargs) -> None:
        preview_called["called"] = True

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    monkeypatch.setattr(processor, "send_preview_to_telegram", _preview)

    processor.process_message(
        account_email="account@example.com",
        message_id=20,
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert len(captured) == 1
    assert preview_called["called"] is False
