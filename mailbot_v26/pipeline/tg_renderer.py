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


def _is_binary_leak(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "data=b'" in lowered or "data=b\"" in lowered:
        return True
    stripped = text.lstrip()
    if stripped.startswith("b'") or stripped.startswith('b"'):
        return True
    printable = 0
    total = len(text)
    for char in text:
        if char in ("\n", "\r", "\t"):
            printable += 1
        elif char.isprintable():
            printable += 1
    if total == 0:
        return False
    printable_ratio = printable / total
    return printable_ratio < 0.7


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
    for attachment in attachments:
        filename = _escape_dynamic(attachment.get("filename") or "attachment")
        extracted_text = _normalize_attachment_text(attachment.get("text"))
        if extracted_text and not _is_binary_leak(extracted_text):
            extracted_text = _truncate_attachment_text(extracted_text)
        else:
            extracted_text = ""
        if extracted_text:
            safe_text = _escape_dynamic(extracted_text)
            lines.append(f"{filename} — <i>{safe_text}</i>")
        else:
            lines.append(f"{filename}")
    return "\n".join(lines)


def build_telegram_text(
    *,
    priority: str,
    from_email: str,
    subject: str,
    action_line: str,
    attachments: list[dict[str, Any]],
) -> str:
    lines = [
        format_priority_line(priority, from_email),
        format_subject(subject),
        format_main_action(action_line),
    ]
    attachments_block = format_attachments_block(attachments)
    if attachments_block:
        lines.append("")
        lines.append(attachments_block)
    return "\n".join(lines)


def build_tg_fallback(
    *,
    priority: str,
    subject: str,
    from_email: str,
    attachments: list[dict[str, Any]],
) -> str:
    lines = [
        format_priority_line(priority, from_email),
        format_subject(subject),
        format_main_action(None),
    ]
    attachments_block = format_attachments_block(attachments)
    if attachments_block:
        lines.append("")
        lines.append(attachments_block)
    return "\n".join(lines)


def build_tg_short_template(*, priority: str, subject: str, from_email: str) -> str:
    return "\n".join(
        [
            format_priority_line(priority, from_email),
            format_subject(subject),
            format_main_action(None),
        ]
    )


__all__ = [
    "build_telegram_text",
    "build_tg_fallback",
    "build_tg_short_template",
    "format_priority_line",
    "format_subject",
    "format_main_action",
    "format_attachments_block",
]
