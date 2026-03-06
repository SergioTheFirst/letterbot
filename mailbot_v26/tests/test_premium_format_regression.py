from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.telegram.decision_trace_ui import build_email_actions_keyboard


def _base_context(**overrides):
    data = {
        "email_id": 500,
        "received_at": datetime(2024, 1, 1, 12, 0),
        "priority": "🟡",
        "from_email": "mss777@mail.ru",
        "subject": "FW: Счет на оплату",
        "action_line": "Ответить",
        "mail_type": "",
        "body_summary": "",
        "body_text": "",
        "attachment_summary": "",
        "attachment_details": [],
        "attachment_files": [{"filename": "Счет.xls", "text": "1405 chars..."}],
        "attachments_count": 1,
        "extracted_text_len": 0,
        "llm_failed": True,
        "signal_invalid": False,
        "insights": [],
        "insight_digest": None,
        "commitments_present": False,
    }
    data.update(overrides)
    return processor.TelegramBuildContext(**data)


def test_xls_attachment_without_llm_renders_full_not_fallback() -> None:
    context = _base_context()

    payload, render_mode, _ = processor.build_telegram_payload(context)

    assert render_mode == processor.TelegramRenderMode.FULL
    assert "Письмо получено" not in payload.html_text
    assert "mss777@mail.ru" in payload.html_text


def test_full_render_contains_priority_circle() -> None:
    payload, _, _ = processor.build_telegram_payload(_base_context())

    assert "🟡" in payload.html_text


def test_full_render_contains_watermark() -> None:
    payload, _, _ = processor.build_telegram_payload(_base_context())

    assert "Powered by" in payload.html_text


def test_full_render_contains_attachment_line() -> None:
    payload, _, _ = processor.build_telegram_payload(
        _base_context(attachment_files=[{"filename": "Счет.xls", "text": ""}])
    )

    assert "📎" in payload.html_text or "Счет.xls" in payload.html_text


def test_initial_keyboard_priority_buttons() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, initial_prio=True)

    labels = [button["text"] for button in keyboard["inline_keyboard"][0]]
    assert "🔴 Срочно" in labels
    assert len(keyboard["inline_keyboard"]) == 1


def test_prio_menu_keeps_back_button() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, prio_menu=True)

    assert keyboard["inline_keyboard"][1][0]["text"] == "↩ Назад"


def test_simple_email_without_llm_renders_full() -> None:
    context = _base_context(
        attachments_count=0,
        attachment_files=[],
        extracted_text_len=42,
        body_text="Прошу оплатить счёт до пятницы",
    )

    _, render_mode, _ = processor.build_telegram_payload(context)

    assert render_mode == processor.TelegramRenderMode.FULL


def test_safe_fallback_only_when_no_display_data() -> None:
    empty_context = _base_context(from_email="", subject="")
    full_context = _base_context(from_email="x@y.ru", subject="Тема")

    _, empty_mode, _ = processor.build_telegram_payload(empty_context)
    _, full_mode, _ = processor.build_telegram_payload(full_context)

    assert empty_mode == processor.TelegramRenderMode.SAFE_FALLBACK
    assert full_mode != processor.TelegramRenderMode.SAFE_FALLBACK


def test_initial_keyboard_shows_human_readable_actions_and_no_trace() -> None:
    payload, _, _ = processor.build_telegram_payload(_base_context())

    keyboard = payload.reply_markup or {}
    rows = keyboard.get("inline_keyboard") or []
    assert len(rows) == 1
    labels = [button["text"] for button in rows[0]]
    assert labels == ["Изменить приоритет", "⏰ Отложить"]
    assert "Почему так?" not in labels
    assert "◀ Скрыть" not in labels


def test_invoice_excel_payload_validation_stays_full() -> None:
    payload, render_mode, _ = processor.build_telegram_payload(
        _base_context(
            mail_type="INVOICE",
            action_line="Проверьте вручную",
            body_summary="",
            body_text="",
            attachment_files=[
                {
                    "filename": "invoice_77.xlsx",
                    "text": "Итого 58200 руб. Оплатить до 28.02.2026",
                }
            ],
            attachments_count=1,
            extracted_text_len=0,
        )
    )

    assert render_mode == processor.TelegramRenderMode.FULL
    assert "📎" in payload.html_text


def test_render_notification_applies_arbiter_without_runtime_error() -> None:
    result = processor._render_notification(
        message_id=77,
        received_at=datetime(2024, 1, 1, 12, 0),
        priority="🟡",
        from_email="billing@example.com",
        from_name="Billing",
        subject="Счет",
        action_line="Проверьте вручную",
        mail_type="INVOICE",
        body_summary="проверить письмо",
        body_text="",
        attachments=[{"filename": "invoice.xlsx", "text": ""}],
        llm_result=SimpleNamespace(failed=False, error=False),
        signal_quality=SimpleNamespace(is_usable=True),
        aggregated_insights=[],
        insight_digest=None,
        telegram_chat_id="chat",
        telegram_bot_token="token",
        account_email="acc@example.com",
        attachment_summaries=[],
        commitments=[],
        enable_premium_clarity=False,
    )

    assert "Автоматическая сводка слишком общая." in result.body_summary

def test_default_mode_sends_only_processed_message() -> None:
    payload, _, _ = processor.build_telegram_payload(_base_context())
    assert "Письмо получено" not in payload.html_text


def test_no_pre_message_in_normal_premium_mode() -> None:
    payload, _, _ = processor.build_telegram_payload(_base_context())
    assert not payload.html_text.startswith("📩 Письмо получено")
