"""Attachment lists show the first 2 files plus "… and N more" (WP1b, D-P2-2).

CONSTITUTION III.6 "show up to 2": 3 or more attachments render the first 2
and an overflow line; the header still counts every attachment. F4: the RU
overflow line used to read "… и ещё N ещё".
"""

from __future__ import annotations

import pytest

from mailbot_v26.pipeline import processor, tg_renderer
from mailbot_v26.ui.i18n import t

_NAMES = ["a.pdf", "b.xlsx", "c.docx", "d.png", "e.txt"]


def _attachments(count: int) -> list[dict[str, str]]:
    return [{"filename": name, "text": ""} for name in _NAMES[:count]]


def test_overflow_key_exists_in_both_locales() -> None:
    assert t("tg.attachments.more", locale="en", count=3) == "… and 3 more"
    assert t("tg.attachments.more", locale="ru", count=3) == "… и ещё 3"


@pytest.mark.parametrize(
    ("locale", "header", "more"),
    [
        ("en", "Attachments: 5 (", "… and 3 more"),
        ("ru", "Вложения: 5 (", "… и ещё 3"),
    ],
)
def test_five_attachments_render_two_lines_and_overflow(locale, header, more) -> None:
    rendered = tg_renderer.format_attachments_block(_attachments(5), locale=locale)

    lines = rendered.split("\n")
    assert lines[0].startswith(header)
    assert lines[1:] == ["a.pdf", "b.xlsx", more]


def test_three_attachments_render_two_plus_one_more() -> None:
    rendered = tg_renderer.format_attachments_block(_attachments(3))

    assert rendered.split("\n")[1:] == ["a.pdf", "b.xlsx", "… and 1 more"]
    assert "c.docx" not in rendered


def test_two_attachments_are_listed_without_overflow() -> None:
    rendered = tg_renderer.format_attachments_block(_attachments(2))

    assert rendered.split("\n")[1:] == ["a.pdf", "b.xlsx"]


@pytest.mark.parametrize(
    ("locale", "header", "more"),
    [
        ("en", "Attachments: 5 (", "… and 3 more"),
        ("ru", "Вложения: 5 (", "… и ещё 3"),
    ],
)
def test_minimal_text_counts_all_and_lists_two(locale, header, more) -> None:
    rendered = tg_renderer.build_minimal_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        attachments=_attachments(5),
        locale=locale,
    )

    lines = rendered.split("\n")
    header_index = next(i for i, line in enumerate(lines) if line.startswith(header))
    assert lines[header_index + 1 :] == ["a.pdf", "b.xlsx", more]
    assert rendered.count(more) == 1
    assert "ещё 3 ещё" not in rendered  # F4


def test_minimal_payload_uses_the_overflow_line_in_ru(monkeypatch) -> None:
    monkeypatch.setattr(processor, "_resolve_outbound_ui_locale", lambda: "ru")

    payload = processor._build_minimal_telegram_payload(
        priority="🔵",
        from_email="sender@example.com",
        subject="Тема",
        attachments=_attachments(5),
        metadata={},
        reply_markup=None,
    )

    lines = payload.html_text.split("\n")
    assert lines[-3:] == ["a.pdf", "b.xlsx", "… и ещё 3"]
    assert any(line.startswith("Вложения: 5 (") for line in lines)
    assert "c.docx" not in payload.html_text


def test_fallback_render_lists_two_attachments() -> None:
    rendered = tg_renderer.build_tg_fallback(
        priority="🔵",
        subject="Subject",
        from_email="sender@example.com",
        attachments=_attachments(4),
    )

    assert rendered.split("\n")[-3:] == ["a.pdf", "b.xlsx", "… and 2 more"]
    assert "c.docx" not in rendered
