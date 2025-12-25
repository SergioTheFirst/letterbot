from __future__ import annotations

from mailbot_v26.pipeline.tg_formatter import (
    format_attachments_block,
    format_main_action,
    format_subject,
)


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

    assert rendered.startswith("📎 Вложений: 3")
    assert "[report.pdf] <i>summary</i>" in rendered
    assert "<i>[report.pdf]" not in rendered
    assert "[scan.png] <i>Текст не извлечён</i>" in rendered
    assert "[notes.txt] <i>" in rendered
    assert "....</i>" in rendered
    assert "\n\n" in rendered
