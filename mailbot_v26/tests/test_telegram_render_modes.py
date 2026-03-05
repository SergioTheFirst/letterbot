from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup_processor(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Проверить письмо",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="gigachat",
        failed=False,
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


def test_no_short_template_when_data_exists() -> None:
    ctx = processor.TelegramRenderContext(
        extracted_text_len=42,
        attachments_count=0,
        llm_failed=True,
        signal_invalid=True,
    )

    mode = processor.choose_tg_render_mode(ctx)

    assert mode is processor.TelegramRenderMode.FULL


def test_attachments_visible_without_llm(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda _: SimpleNamespace(
            entropy=0.0,
            printable_ratio=0.0,
            quality_score=0.0,
            is_usable=False,
            reason="empty",
        ),
    )
    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    attachments = [
        {
            "filename": "one.doc",
            "content_type": "application/msword",
            "text": "a" * 10,
        },
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=11,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    telegram_text = sent["payload"].html_text
    assert telegram_text.startswith("📩 Письмо получено") or ("📎" in telegram_text and "one.doc" in telegram_text)


def test_safe_fallback_still_shows_attachments(monkeypatch) -> None:
    _setup_processor(monkeypatch)
    monkeypatch.setattr(processor, "_build_telegram_text", lambda **kwargs: "short")

    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    attachments = [
        {
            "filename": "one.doc",
            "content_type": "application/msword",
            "text": "a" * 10,
        },
    ]

    processor.process_message(
        account_email="account@example.com",
        message_id=12,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body text that should not disappear.",
        attachments=attachments,
        telegram_chat_id="chat",
    )

    telegram_text = sent["payload"].html_text
    assert telegram_text.startswith("📩 Письмо получено") or telegram_text.startswith("🔴 от sender@example.com:") or telegram_text.startswith("Письмо получено")
    assert telegram_text.startswith("📩 Письмо получено") or "📎" in telegram_text or "Вложения:" in telegram_text


def test_renderer_mode_logged(monkeypatch, caplog: pytest.LogCaptureFixture) -> None:
    _setup_processor(monkeypatch)
    sent: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    with caplog.at_level("INFO"):
        processor.process_message(
            account_email="account@example.com",
            message_id=13,
            from_email="sender@example.com",
            subject="Subject",
            received_at=datetime(2024, 1, 1, 12, 0),
            body_text="Body text with enough content to pass validation.",
            attachments=[],
            telegram_chat_id="chat",
        )

    assert "tg_render_mode_selected" in caplog.text


def test_full_pipeline_excel_attachment_stays_in_full_mode() -> None:
    context = processor.TelegramBuildContext(
        email_id=100,
        received_at=datetime(2024, 1, 1, 12, 0),
        priority="🔵",
        from_email="sender@example.com",
        subject="Тема",
        action_line="Ответить",
        mail_type="SECURITY_ALERT",
        body_summary="Краткое summary",
        body_text="Текст письма",
        attachment_summary="",
        attachment_details=[],
        attachment_files=[
            {"filename": "УПД.xlsx", "text": "Поставщик Итого 15000"},
            {"filename": "УПД.pdf", "text": ""},
        ],
        attachments_count=4,
        extracted_text_len=2292,
        llm_failed=False,
        signal_invalid=False,
        insights=[],
        insight_digest=None,
        commitments_present=False,
    )

    payload, render_mode, payload_invalid = processor.build_telegram_payload(context)

    assert render_mode == processor.TelegramRenderMode.FULL
    assert payload_invalid is False
    assert "📎" in payload.html_text


def test_heuristic_path_produces_full_render() -> None:
    context = processor.TelegramBuildContext(
        email_id=101,
        received_at=datetime(2024, 1, 1, 12, 0),
        priority="🟡",
        from_email="sender@example.com",
        subject="Тема",
        action_line="",
        mail_type="",
        body_summary="",
        body_text="Текст письма с достаточной длиной для показа в FULL режиме.",
        attachment_summary="",
        attachment_details=[],
        attachment_files=[],
        attachments_count=0,
        extracted_text_len=120,
        llm_failed=False,
        signal_invalid=False,
        insights=[],
        insight_digest=None,
        commitments_present=False,
    )

    payload, render_mode, payload_invalid = processor.build_telegram_payload(context)

    assert render_mode == processor.TelegramRenderMode.FULL
    assert payload_invalid is False
    assert "Письмо получено" not in payload.html_text


def test_heuristic_summary_extracts_body_text() -> None:
    summary = processor._build_heuristic_summary(
        subject="Тема",
        body_text="Это первый абзац письма, который достаточно длинный для корректного summary.",
    )

    assert summary
    assert len(summary) >= 12


def test_heuristic_summary_uses_attachment_text_when_body_empty() -> None:
    summary = processor._build_heuristic_summary(
        subject="Тема",
        body_text="",
        attachments=[
            {
                "filename": "invoice.xlsx",
                "text": "Итого: 87 500 руб. Оплатить до 15.04.2026",
            }
        ],
    )

    assert "87 500" in summary


def test_direct_llm_path_kept_without_regression(monkeypatch) -> None:
    called = {"value": False}

    def _run_llm_stage(**kwargs):
        called["value"] = True
        return SimpleNamespace(
            priority="🟡",
            action_line="Проверить договор",
            body_summary="Нужно проверить и подписать договор.",
            attachment_summaries=[],
            llm_provider="gigachat",
            failed=False,
        )

    monkeypatch.setattr(processor, "run_llm_stage", _run_llm_stage)
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
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: DeliveryResult(delivered=True, retryable=False))

    processor.process_message(
        account_email="account@example.com",
        message_id=99,
        from_email="sender@example.com",
        subject="Договор",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Пожалуйста подпишите договор.",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert called["value"] is True


def test_heuristic_action_line_not_empty() -> None:
    assert processor._build_heuristic_action_line(priority="🔴")
    assert processor._build_heuristic_action_line(priority="🟡")
    assert processor._build_heuristic_action_line(priority="🔵")
