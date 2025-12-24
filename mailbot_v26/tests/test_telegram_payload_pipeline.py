from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker.telegram_sender import TelegramSendResult


def _setup_processor(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
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


def _capture_payload(monkeypatch) -> dict[str, TelegramPayload]:
    captured: dict[str, TelegramPayload] = {}

    def _enqueue_tg(*, email_id: int, payload: TelegramPayload) -> None:
        captured["payload"] = payload
        return TelegramSendResult(success=True)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    return captured


def test_telegram_contains_attachment_summary(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    captured = _capture_payload(monkeypatch)
    attachments = [
        {
            "filename": "doc1.doc",
            "content_type": "application/msword",
            "text": "a" * 128,
        }
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    html_text = captured["payload"].html_text
    assert "Вложения: 1" in html_text
    assert "DOC" in html_text


def test_extracted_text_visible_in_tg(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    captured = _capture_payload(monkeypatch)
    body_text = "Важно: оплатить счет до пятницы."

    processor.process_message(
        account_email="account@example.com",
        message_id=2,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 2, 12, 0),
        body_text=body_text,
        attachments=[],
        telegram_chat_id="chat",
    )

    html_text = captured["payload"].html_text
    assert body_text in html_text


def test_no_minimal_template_when_attachments_exist(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    captured = _capture_payload(monkeypatch)
    attachments = [
        {
            "filename": "report.pdf",
            "content_type": "application/pdf",
            "text": "summary",
        }
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=3,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 3, 12, 0),
        body_text="",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    html_text = captured["payload"].html_text
    assert "ℹ️ Детали будут доступны позже." not in html_text
    assert "Вложения: 1" in html_text


def test_empty_body_with_attachments_uses_fallback(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    captured = _capture_payload(monkeypatch)
    attachments = [
        {
            "filename": "doc1.docx",
            "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "text": "",
        },
        {
            "filename": "report.pdf",
            "content_type": "application/pdf",
            "text": "",
        },
        {
            "filename": "table.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": "",
        },
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=4,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 4, 12, 0),
        body_text="",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    html_text = captured["payload"].html_text
    assert html_text.startswith("Получено письмо с 3 вложениями.")
    assert "Типы файлов" in html_text
    assert "DOC" in html_text
    assert "PDF" in html_text
    assert "XLS" in html_text


def test_empty_summary_uses_fallback(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary="",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    captured = _capture_payload(monkeypatch)

    processor.process_message(
        account_email="account@example.com",
        message_id=5,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 5, 12, 0),
        body_text="Body text that should not disappear.",
        attachments=[],
        telegram_chat_id="chat",
    )

    html_text = captured["payload"].html_text
    assert html_text.startswith("Получено письмо")
