from __future__ import annotations

from typing import Any

from mailbot_v26.telegram_utils import escape_tg_html

_ATTACHMENT_SNIPPET_LIMIT = 240


def _escape_dynamic(text: str | None) -> str:
    return escape_tg_html(str(text or ""))


def _normalize_attachment_text(text: Any) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode(errors="ignore")
    return " ".join(str(text).split())


def _truncate_attachment_text(text: str) -> str:
    if len(text) <= _ATTACHMENT_SNIPPET_LIMIT:
        return text
    truncated = text[: _ATTACHMENT_SNIPPET_LIMIT - 4].rstrip()
    return f"{truncated}...."


def format_priority_line(priority: str, from_email: str) -> str:
    safe_sender = _escape_dynamic(from_email or "неизвестно")
    return f"{priority} от {safe_sender}:"


def format_subject(subject: str) -> str:
    safe_subject = _escape_dynamic(subject or "(без темы)")
    return f"<b>{safe_subject}</b>"


def format_main_action(action_line: str | None) -> str:
    cleaned = (action_line or "").strip()
    if not cleaned:
        cleaned = "Действий не требуется"
    safe_action = _escape_dynamic(cleaned)
    return f"<b><i>{safe_action}</i></b>"


def format_attachments_block(attachments: list[dict[str, Any]]) -> str:
    if not attachments:
        return ""
    lines = [f"📎 Вложений: {len(attachments)}"]
    for index, attachment in enumerate(attachments):
        filename = _escape_dynamic(attachment.get("filename") or "attachment")
        extracted_text = _normalize_attachment_text(attachment.get("text"))
        if not extracted_text:
            extracted_text = "Текст не извлечён"
        extracted_text = _truncate_attachment_text(extracted_text)
        safe_text = _escape_dynamic(extracted_text)
        lines.append(f"[{filename}] <i>{safe_text}</i>")
        if index < len(attachments) - 1:
            lines.append("")
    return "\n".join(lines)


__all__ = [
    "format_priority_line",
    "format_subject",
    "format_main_action",
    "format_attachments_block",
]
