from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor


def test_telegram_payload_unchanged(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Позвонить клиенту",
        body_summary="Summary",
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

    payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: payload.update(kwargs))

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

    assert set(payload.keys()) == {
        "chat_id",
        "priority",
        "from_email",
        "subject",
        "action_line",
        "body_summary",
        "attachment_summaries",
        "account_email",
    }
