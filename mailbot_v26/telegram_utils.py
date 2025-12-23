"""Utilities for safe Telegram HTML payloads."""
from __future__ import annotations

import html


def telegram_safe(text: str) -> str:
    """Escape Telegram HTML entities and remove backslashes."""
    cleaned = (text or "").replace("\\", "")
    return html.escape(cleaned, quote=True)


__all__ = ["telegram_safe"]
