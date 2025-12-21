import json
import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Оплатить счет",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def test_commitments_persist_failed_logs(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
        ENABLE_COMMITMENT_TRACKER=True,
    )
    monkeypatch.setattr(processor, "feature_flags", flags)
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Оплатить счет", "shadow action reason")],
    )
    monkeypatch.setattr(
        processor.auto_action_engine,
        "propose",
        lambda **kwargs: {
            "type": "PAYMENT",
            "text": "Оплатить счет",
            "source": "shadow",
            "confidence": 0.9,
        },
    )

    def _raise_commitments(**kwargs) -> None:
        raise RuntimeError("db down")

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=_raise_commitments,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
        ),
    )
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: None)

    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=601,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 7, 1, 12, 0),
        body_text="Вышлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert any(
        json.loads(record.message).get("event") == "commitments_persist_failed"
        for record in caplog.records
        if record.message.startswith("{")
    )


def test_commitment_status_update_crm_error_logs(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
        ENABLE_COMMITMENT_TRACKER=True,
    )
    monkeypatch.setattr(processor, "feature_flags", flags)
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Оплатить счет", "shadow action reason")],
    )
    monkeypatch.setattr(
        processor.auto_action_engine,
        "propose",
        lambda **kwargs: {
            "type": "PAYMENT",
            "text": "Оплатить счет",
            "source": "shadow",
            "confidence": 0.9,
        },
    )

    def _raise_fetch(**kwargs) -> None:
        raise RuntimeError("crm failure")

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=_raise_fetch,
            update_commitment_statuses=lambda **kwargs: True,
        ),
    )
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: None)

    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=602,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 7, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert any(
        json.loads(record.message).get("event") == "commitment_status_update_failed"
        for record in caplog.records
        if record.message.startswith("{")
    )
