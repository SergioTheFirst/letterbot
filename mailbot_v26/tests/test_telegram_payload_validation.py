from __future__ import annotations

import sqlite3
from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup_processor(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Проверить письмо",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
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
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: (llm_priority, None),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [],
    )
    monkeypatch.setattr(processor.context_store, "resolve_sender_entity", lambda **kwargs: None)
    monkeypatch.setattr(processor.context_store, "record_interaction_event", lambda **kwargs: (None, None))
    monkeypatch.setattr(processor.context_store, "recompute_email_frequency", lambda **kwargs: (0.0, 0))
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_args, **_kwargs: SimpleNamespace(
            entropy=1.0,
            printable_ratio=1.0,
            quality_score=1.0,
            is_usable=True,
            reason="ok",
        ),
    )


def _load_event_types(db_path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT type FROM events").fetchall()
    return [row[0] for row in rows]


def test_tg_payload_with_attachments(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    attachments = [
        {
            "filename": "one.doc",
            "content_type": "application/msword",
            "text": "a" * 1143,
        },
        {
            "filename": "two.xls",
            "content_type": "application/vnd.ms-excel",
            "text": "b" * 1608,
        },
        {
            "filename": "three.pdf",
            "content_type": "application/pdf",
            "text": "c" * 746,
        },
        {
            "filename": "four.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": "d" * 2234,
        },
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body text with enough content to pass validation.",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    telegram_text = sent["payload"].html_text
    assert "Вложения: 4" in telegram_text
    assert "DOC" in telegram_text
    assert "XLS" in telegram_text
    assert "PDF" in telegram_text


def test_tg_payload_never_subject_only(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    monkeypatch.setattr(processor, "_build_telegram_text", lambda **kwargs: "Subject")

    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=2,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent["payload"].html_text.startswith("Получено письмо")


def test_tg_payload_validator_blocks_empty(monkeypatch, caplog: pytest.LogCaptureFixture) -> None:
    _setup_processor(monkeypatch)
    monkeypatch.setattr(processor, "_build_telegram_text", lambda **kwargs: "short")

    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    with caplog.at_level("WARNING"):
        processor.process_message(
            account_email="account@example.com",
            message_id=3,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert "payload_validation_failed" in caplog.text
    assert sent["payload"].html_text.startswith("Получено письмо")


def test_pipeline_does_not_mark_success_on_invalid_tg(monkeypatch, tmp_path) -> None:
    _setup_processor(monkeypatch)
    monkeypatch.setattr(processor, "_build_telegram_text", lambda **kwargs: "short")
    emitter = EventEmitter(tmp_path / "events.sqlite")
    monkeypatch.setattr(processor, "event_emitter", emitter)

    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=4,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert "payload_validation_failed" in _load_event_types(emitter.path)


def test_payload_escapes_angle_brackets(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=5,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Связаться с <mail@host> для деталей.",
        attachments=[],
        telegram_chat_id="chat",
    )

    html_text = sent["payload"].html_text
    assert "<mail@host>" not in html_text
    assert "&lt;mail@host&gt;" in html_text


def test_attachment_visibility(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Проверить письмо",
        body_summary="Check email",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_args, **_kwargs: SimpleNamespace(
            entropy=0.0,
            printable_ratio=0.0,
            quality_score=0.0,
            is_usable=False,
            reason="low_signal",
        ),
    )
    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    attachments = [
        {
            "filename": "report.pdf",
            "content_type": "application/pdf",
            "text": "attachment text",
            "size_bytes": 2048,
        }
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=6,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body text with enough content to pass validation.",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    telegram_text = sent["payload"].html_text
    assert "Attachments:" in telegram_text
    assert "report.pdf" in telegram_text
    assert "Manual Review: Important attachments found." in telegram_text


def test_retry_trigger(monkeypatch) -> None:
    _setup_processor(monkeypatch)

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        return DeliveryResult(delivered=False, retryable=True, error="http 500")

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    with pytest.raises(RuntimeError):
        processor.process_message(
            account_email="account@example.com",
            message_id=7,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body text with enough content to pass validation.",
            attachments=[],
            telegram_chat_id="chat",
        )
