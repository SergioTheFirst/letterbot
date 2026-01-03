from __future__ import annotations

import re
from collections import Counter
from typing import Any

from mailbot_v26.telegram_utils import escape_tg_html
from mailbot_v26.ui.emoji_whitelist import strip_disallowed_emojis
from mailbot_v26.text.sanitize import is_binaryish

_ATTACHMENT_SNIPPET_LIMIT = 240
_BASE64_FRAGMENT = re.compile(r"[A-Za-z0-9+/]{80,}={0,2}")


def _escape_dynamic(text: str | None) -> str:
    cleaned = strip_disallowed_emojis(str(text or ""))
    return escape_tg_html(cleaned)


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
    if is_binaryish(text):
        return True
    if _BASE64_FRAGMENT.search(text):
        if any(char in text for char in "+/=") or any(char.isdigit() for char in text):
            return True
    if len(text) >= 60:
        base64ish = sum(1 for char in text if char.isalnum() or char in "+/=")
        if base64ish / len(text) > 0.9 and any(
            char.isdigit() or char in "+/=" for char in text
        ):
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


def _attachment_skipped_reason(attachment: dict[str, Any]) -> str | None:
    reason = attachment.get("skipped_reason")
    if reason:
        return str(reason)
    metadata = attachment.get("metadata")
    if isinstance(metadata, dict):
        reason = metadata.get("skipped_reason")
        if reason:
            return str(reason)
    return None


def _attachment_size_bytes(attachment: dict[str, Any]) -> int:
    for key in ("size_bytes", "size"):
        value = attachment.get(key)
        if isinstance(value, int):
            return value
    return 0


def _format_size_mb(size_bytes: int) -> str:
    if size_bytes <= 0:
        return "0 МБ"
    size_mb = size_bytes / (1024 * 1024)
    formatted = f"{size_mb:.1f}".rstrip("0").rstrip(".")
    return f"{formatted} МБ"


def _attachment_type_label(filename: str) -> str:
    cleaned = (filename or "").strip()
    if "." not in cleaned:
        return "ДРУГОЕ"
    ext = cleaned.rsplit(".", 1)[-1].lower()
    ext = re.sub(r"[^a-z0-9]+", "", ext)
    if not ext:
        return "ДРУГОЕ"
    normalized = {
        "jpeg": "JPG",
        "jpg": "JPG",
    }.get(ext, ext.upper())
    return normalized


def _attachment_type_summary(attachments: list[dict[str, Any]]) -> str:
    counts = Counter(
        _attachment_type_label(str(attachment.get("filename") or ""))
        for attachment in attachments
    )
    summary_parts = [f"{ext}×{count}" for ext, count in sorted(counts.items())]
    summary = ", ".join(summary_parts)
    if summary:
        return f"Вложения: {len(attachments)} ({summary})"
    return f"Вложения: {len(attachments)}"


def format_priority_line(priority: str, from_email: str) -> str:
    if priority not in {"🔴", "🟡", "🔵"}:
        priority = "🔵"
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


def format_narrative_block(
    *,
    fact: str,
    context: str | None,
    action: str | None,
) -> str:
    lines = [f"<b>Факт:</b> <i>{_escape_dynamic(fact)}</i>"]
    if context:
        lines.append(f"<b>Контекст:</b> <i>{_escape_dynamic(context)}</i>")
    if action:
        lines.append(f"<b>Действие:</b> <i>{_escape_dynamic(action)}</i>")
    return "\n".join(lines)


def format_attachments_block(attachments: list[dict[str, Any]]) -> str:
    if not attachments:
        return ""
    lines = [_attachment_type_summary(attachments)]
    for attachment in attachments:
        filename = _escape_dynamic(attachment.get("filename") or "вложение")
        skipped_reason = _attachment_skipped_reason(attachment)
        if skipped_reason == "too_large":
            size_display = _format_size_mb(_attachment_size_bytes(attachment))
            lines.append(
                f"{filename} — <i>слишком большой файл ({size_display}), извлечение отключено</i>"
            )
            continue
        if skipped_reason == "total_limit":
            lines.append(f"{filename} — <i>пропущен из-за ограничения размера письма</i>")
            continue
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
    "format_narrative_block",
    "format_attachments_block",
]
