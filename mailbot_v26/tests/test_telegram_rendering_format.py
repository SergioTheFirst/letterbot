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

    assert rendered.startswith("Вложения: 3 (PDF×1, PNG×1, TXT×1)")
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
        body_summary="",
        body_text="",
        attachments=[],
        attachment_summary="Позвонить клиенту сегодня",
    )

    matches = [
        line
        for line in rendered.splitlines()
        if "Позвонить клиенту сегодня" in line
    ]
    assert len(matches) == 1
