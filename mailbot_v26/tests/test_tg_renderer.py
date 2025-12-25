from __future__ import annotations

from mailbot_v26.pipeline import tg_renderer


def test_tg_render_standard() -> None:
    attachments = [
        {"filename": "report.pdf", "text": "summary"},
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Проверить письмо",
        attachments=attachments,
    )

    assert "📎 Вложений: 1" in rendered
    assert "<b><i>Проверить письмо</i></b>" in rendered


def test_tg_no_brackets() -> None:
    attachments = [
        {"filename": "doc1.doc", "text": "snippet"},
    ]

    rendered = tg_renderer.format_attachments_block(attachments)

    assert "[doc1.doc]" not in rendered
    assert "doc1.doc — <i>snippet</i>" in rendered


def test_binary_suppression() -> None:
    attachments = [
        {"filename": "dump.bin", "text": "data=b'\\x00\\x01\\x02'"},
        {"filename": "raw.bin", "text": "b'\\x00\\x01\\x02'"},
    ]

    rendered = tg_renderer.format_attachments_block(attachments)

    assert "dump.bin" in rendered
    assert "raw.bin" in rendered
    assert "dump.bin — <i>" not in rendered
    assert "raw.bin — <i>" not in rendered
    assert "data=b'\\x00" not in rendered
