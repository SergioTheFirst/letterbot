from __future__ import annotations

from datetime import datetime

from mailbot_v26.pipeline import processor
from mailbot_v26.ui.branding import WATERMARK_LINE


def _render(
    *,
    priority: str = "🟡",
    action_line: str = "Ответить клиенту",
    attachments: list[dict[str, object]] | None = None,
    body_summary: str = "Первая строка.\nВторая строка.\nТретья строка.\nЧетвертая строка.",
) -> str:
    return processor._build_premium_clarity_text(
        priority=priority,
        received_at=datetime(2026, 1, 1),
        from_email="sender@example.com",
        from_name="Sender",
        subject="Тема письма",
        action_line=action_line,
        body_summary=body_summary,
        body_text=body_summary,
        attachments=attachments or [],
        attachment_summaries=[],
        insights=[],
        insight_digest=None,
        commitments=[],
        attachments_count=len(attachments or []),
        extracted_text_len=120,
        confidence_percent=80,
        confidence_available=True,
        confidence_dots_mode="auto",
        confidence_dots_threshold=75,
        confidence_dots_scale=10,
        extraction_failed=False,
    )


def test_premium_clarity_default_layout_matches_target() -> None:
    rendered = _render(attachments=[{"filename": "invoice.pdf", "text": ""}])
    lines = rendered.splitlines()

    assert lines[0] == "🟡 от sender@example.com:"
    assert lines[1] == "Тема письма"
    assert lines[2] == "Ответить"
    assert lines[3] == ""
    assert lines[4] == "📎 1 вложение: invoice.pdf"
    assert lines[5:8] == ["Первая строка.", "Вторая строка.", "Третья строка."]
    assert WATERMARK_LINE in rendered


def test_premium_clarity_default_layout_does_not_include_fallback_noise() -> None:
    rendered = _render(attachments=[{"filename": "invoice.pdf", "text": ""}])

    assert "🟡 Письмо" not in rendered
    assert "От:" not in rendered
    assert "Тема:" not in rendered
    assert "Подробнее:" not in rendered
    assert "Attention Needed" not in rendered


def test_premium_clarity_first_line_keeps_colored_priority_dot() -> None:
    assert _render(priority="🔴").splitlines()[0].startswith("🔴 ")
    assert _render(priority="🟡").splitlines()[0].startswith("🟡 ")
    assert _render(priority="🔵").splitlines()[0].startswith("🔵 ")


def test_premium_clarity_action_shortcuts() -> None:
    assert _render(action_line="Нужно ответить сегодня").splitlines()[2] == "Ответить"
    assert _render(action_line="Оплатить счёт до завтра").splitlines()[2] == "Оплатить"
    assert _render(action_line="Проверка данных").splitlines()[2] == "Проверить"


def test_premium_clarity_default_without_attachments() -> None:
    rendered = _render(attachments=[])
    assert "📎 0 вложений" in rendered
