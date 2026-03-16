from __future__ import annotations

from mailbot_v26.pipeline.tg_renderer import (
    format_attachments_block,
    format_main_action,
    format_subject,
)
from mailbot_v26.pipeline import processor
def test_subject_is_html_escaped() -> None:
    rendered = format_subject("Subject <tag> & more")
    assert "<tag>" not in rendered
    assert "&lt;tag&gt;" in rendered
    assert "&amp;" in rendered


def test_action_is_bold_italic_only() -> None:
    rendered = format_main_action("Сделать что-то")
    assert rendered.startswith("<b><i>")
    assert rendered.endswith("</i></b>")


def test_attachments_block_formatting_and_truncation() -> None:
    long_text = "a" * 300
    attachments = [
        {"filename": "report.pdf", "text": "summary"},
        {"filename": "scan.png", "text": ""},
        {"filename": "notes.txt", "text": long_text},
    ]

    rendered = format_attachments_block(attachments)

    assert rendered.startswith("Attachments: 3 (PDF×1, PNG×1, TXT×1)")
    assert "report.pdf — <i>summary</i>" in rendered
    assert "<i>report.pdf" not in rendered
    assert "\nscan.png\n" in f"\n{rendered}\n"
    assert "notes.txt — <i>" in rendered
    assert "....</i>" in rendered
    assert "\n\n" not in rendered


def test_full_message_no_duplicate_lines() -> None:
    rendered = processor._build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Позвонить клиенту сегодня",
        mail_type="",
        body_summary="",
        body_text="",
        attachments=[],
        attachment_summary="Позвонить клиенту сегодня",
    )

    matches = [
        line for line in rendered.splitlines() if "Позвонить клиенту сегодня" in line
    ]
    assert len(matches) == 1


def test_processor_format_drops_duplicate_subject_first_body_line_with_fw_re() -> None:
    rendered = processor._build_telegram_text(
        priority="🔴",
        from_email="sender@example.com",
        subject="FW: Счет",
        action_line="RE:   счет",
        mail_type="",
        body_summary="",
        body_text="",
        attachments=[],
        attachment_summary="",
    )

    assert rendered.splitlines() == ["🔴 from sender@example.com — FW: Счет"]


def test_signal_hints_are_single_per_type() -> None:
    insights = [
        processor.Insight(
            type="silence",
            severity="MEDIUM",
            explanation="Контакт молчит 9 дней",
            recommendation="",
        ),
        processor.Insight(
            type="silence",
            severity="HIGH",
            explanation="Контакт молчит 10 дней",
            recommendation="",
        ),
        processor.Insight(
            type="deadlock",
            severity="MEDIUM",
            explanation="3-е письмо без ответа",
            recommendation="",
        ),
    ]

    hints = processor._build_signal_hints(insights)

    assert len(hints) == 2
    assert hints[0].startswith("⚠ ")
    assert hints[1].startswith("🔁 ")


def test_processor_build_telegram_text_uses_mail_type_for_attachment_insight() -> None:
    rendered = processor._build_telegram_text(
        priority="🔴",
        from_email="sender@example.com",
        subject="Счет",
        action_line="Оплатить",
        mail_type="INVOICE",
        body_summary="",
        body_text="",
        attachments=[{"filename": "bill.pdf", "text": "Сумма 58200 руб до 28.02.2026"}],
        attachment_summary=None,
    )

    assert "📎 58 200 ₽ · due 28.02" in rendered
    assert "bill.pdf" not in rendered


def test_watermark_in_full_render() -> None:
    rendered = processor._build_telegram_text(
        priority="🟡",
        from_email="sender@example.com",
        subject="Тема",
        action_line="Ответить",
        body_summary="Краткая сводка для проверки watermark.",
        body_text="Текст письма",
        attachments=[],
        attachment_summary=None,
    )

    assert "Powered by" not in rendered
