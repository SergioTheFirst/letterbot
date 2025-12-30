from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline import processor as processor_module


def test_tg_payload_with_attachments():
    attachments = [
        {"filename": "doc1.doc", "content_type": "application/msword", "text": "a" * 1143},
        {
            "filename": "sheet1.xls",
            "content_type": "application/vnd.ms-excel",
            "text": "b" * 1608,
        },
        {"filename": "file.pdf", "content_type": "application/pdf", "text": "c" * 746},
        {
            "filename": "sheet2.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": "d" * 2234,
        },
    ]
    details = processor_module._build_attachment_details(attachments)
    summary = processor_module._build_attachment_summary(details)
    text = processor_module._build_telegram_text(
        priority="🔵",
        from_email="hq@example.com",
        subject="FW: КАРАВАЙ СТАРЛАЙН",
        action_line="Проверить письмо",
        body_summary="Документы приложены, пожалуйста, проверьте.",
        body_text="",
        attachment_summary=summary,
    )
    ctx = processor_module.EmailContext(
        subject="FW: КАРАВАЙ СТАРЛАЙН",
        from_email="hq@example.com",
        body_text="Документы приложены",
        attachments_count=len(attachments),
    )

    validated = processor_module.validate_tg_payload(text, ctx)

    assert "Вложения: 4" in validated
    assert "- DOC: 1143 chars" in validated
    assert "- XLS: 1608 chars" in validated
    assert "- PDF: 746 chars" in validated
    assert "- XLS: 2234 chars" in validated


def test_tg_payload_never_subject_only():
    subject = "Очень длинная тема письма для проверки деградации"
    payload = subject
    ctx = processor_module.EmailContext(
        subject=subject,
        from_email="hq@example.com",
        body_text="Полное тело письма должно быть доступно.",
        attachments_count=0,
    )

    with pytest.raises(processor_module.InvalidTelegramPayload):
        processor_module.validate_tg_payload(payload, ctx)

    fallback = processor_module._build_tg_fallback(
        subject=subject,
        from_email="hq@example.com",
        attachment_summary="",
    )

    assert "Письмо получено" in fallback
    assert "Основной текст не удалось безопасно отобразить." in fallback
    assert "Вложения: 0" in fallback


def test_tg_payload_validator_blocks_empty():
    ctx = processor_module.EmailContext(
        subject="Short subject",
        from_email="hq@example.com",
        body_text="",
        attachments_count=0,
    )

    with pytest.raises(processor_module.InvalidTelegramPayload, match="too short"):
        processor_module.validate_tg_payload("Коротко", ctx)


def test_pipeline_does_not_mark_success_on_invalid_tg(monkeypatch, caplog):
    emitted_events: list[dict] = []
    captured: dict[str, str] = {}

    def fake_emit(self, **kwargs):
        emitted_events.append(kwargs)

    def fake_enqueue_tg(*, email_id: int, payload):
        captured["telegram_text"] = payload.html_text

    def fake_validate(text, ctx):
        raise processor_module.InvalidTelegramPayload("attachments missing")

    def fake_run_llm_stage(**kwargs):
        return SimpleNamespace(
            priority="🔵",
            action_line="Проверить",
            body_summary="",
            attachment_summaries=[],
            llm_provider="test",
            llm_model="mock",
            prompt_full="",
            response_full="",
        )

    monkeypatch.setattr(processor_module, "validate_tg_payload", fake_validate)
    monkeypatch.setattr(processor_module, "enqueue_tg", fake_enqueue_tg)
    monkeypatch.setattr(processor_module.EventEmitter, "emit", fake_emit)
    monkeypatch.setattr(processor_module, "run_llm_stage", fake_run_llm_stage)
    monkeypatch.setattr(processor_module.context_store, "resolve_sender_entity", lambda **kwargs: None)
    monkeypatch.setattr(processor_module.shadow_priority_engine, "compute", lambda **kwargs: ("🔵", None))
    monkeypatch.setattr(processor_module.shadow_action_engine, "compute", lambda **kwargs: [])
    monkeypatch.setattr(
        processor_module.DecisionTraceWriter, "write", lambda self, **kwargs: None
    )
    monkeypatch.setattr(
        processor_module.SystemHealthSnapshotter, "maybe_log", lambda self: None
    )
    monkeypatch.setattr(processor_module.knowledge_db, "save_email", lambda **kwargs: 1)

    with caplog.at_level("WARNING"):
        processor_module.process_message(
            account_email="account@example.com",
            message_id=1,
            from_email="hq@example.com",
            from_name="HQ",
            subject="Тестовое письмо",
            received_at=datetime.now(timezone.utc),
            body_text="Полное тело письма.",
            attachments=[],
            telegram_chat_id="123",
        )

    assert "tg_payload_invalid" in caplog.text
    assert captured.get("telegram_text", "").startswith("🔵 от hq@example.com:")
    assert any(event.get("type") == "tg_payload_invalid" for event in emitted_events)
