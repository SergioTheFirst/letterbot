"""Utilities for safe Telegram HTML payloads."""
from __future__ import annotations

import html


def telegram_safe(text: str) -> str:
    """Escape Telegram HTML entities and remove backslashes."""
    cleaned = (text or "").replace("\\", "")
    allowed_tags = {
        "<b>": "__TG_TAG_B_OPEN__",
        "</b>": "__TG_TAG_B_CLOSE__",
        "<i>": "__TG_TAG_I_OPEN__",
        "</i>": "__TG_TAG_I_CLOSE__",
    }
    for tag, placeholder in allowed_tags.items():
        cleaned = cleaned.replace(tag, placeholder)
    escaped = html.escape(cleaned, quote=True)
    for tag, placeholder in allowed_tags.items():
        escaped = escaped.replace(placeholder, tag)
    return escaped


__all__ = ["telegram_safe"]
