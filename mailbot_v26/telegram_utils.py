"""Utilities for safe Telegram HTML payloads."""

from __future__ import annotations

import re

# sendMessage / editMessageText reject longer texts with a non-retryable 400.
TELEGRAM_TEXT_LIMIT = 4096
_CUT_MARK = "…"
_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9-]*)[^>]*>")
_ANY_TAG_RE = re.compile(r"<[^>]*(?:>|$)")
_PARTIAL_ENTITY_RE = re.compile(r"&#?[a-zA-Z0-9]*$")


def escape_tg_html(text: str) -> str:
    """Escape Telegram HTML entities for dynamic fields."""
    cleaned = text or ""
    cleaned = re.sub(r"&(?!(?:[a-zA-Z]+|#\d+);)", "&amp;", cleaned)
    cleaned = cleaned.replace("<", "&lt;").replace(">", "&gt;")
    return cleaned


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
    escaped = escape_tg_html(cleaned)
    for tag, placeholder in allowed_tags.items():
        escaped = escaped.replace(placeholder, tag)
    return escaped


def telegram_text_units(text: str) -> int:
    """Text length as Telegram counts it: UTF-16 code units."""
    return len((text or "").encode("utf-16-le")) // 2


def _open_tags_after(open_tags: list[str], line: str) -> list[str]:
    opened = list(open_tags)
    for match in _TAG_RE.finditer(line):
        name = match.group(2).lower()
        if not match.group(1):
            if not match.group(0).endswith("/>"):
                opened.append(name)
        elif name in opened:
            del opened[len(opened) - 1 - opened[::-1].index(name) :]
    return opened


def _closing_tags(open_tags: list[str]) -> str:
    return "".join(f"</{name}>" for name in reversed(open_tags))


def _cut_as_plain_text(line: str, budget: int) -> str:
    kept: list[str] = []
    used = 0
    for char in _ANY_TAG_RE.sub("", line):
        width = 2 if ord(char) > 0xFFFF else 1
        if used + width > budget:
            break
        kept.append(char)
        used += width
    return _PARTIAL_ENTITY_RE.sub("", "".join(kept))


def cap_telegram_text(html: str, limit: int = TELEGRAM_TEXT_LIMIT) -> str:
    """Fit Telegram HTML into ``limit`` UTF-16 units (unchanged when it fits).

    Keeps whole lines while they fit (closing tags those lines left open),
    then the next line cut as plain text (tags stripped, no partial entity),
    and marks the cut with an ellipsis.
    """
    text = html or ""
    if telegram_text_units(text) <= limit:
        return text
    mark_units = telegram_text_units(_CUT_MARK)
    lines = text.split("\n")
    kept: list[str] = []
    open_tags: list[str] = []
    used = 0
    for line in lines:
        cost = telegram_text_units(line) + (1 if kept else 0)
        opened = _open_tags_after(open_tags, line)
        closing = telegram_text_units(_closing_tags(opened))
        # Room for the closing tags, a newline and the cut mark must remain.
        if used + cost + closing + 1 + mark_units > limit:
            break
        kept.append(line)
        open_tags = opened
        used += cost
    parts: list[str] = []
    budget = limit - mark_units
    if kept:
        head = "\n".join(kept) + _closing_tags(open_tags)
        parts.append(head)
        budget -= telegram_text_units(head) + 1
    next_line = lines[len(kept)] if len(kept) < len(lines) else ""
    parts.append(_cut_as_plain_text(next_line, budget) + _CUT_MARK)
    return "\n".join(parts)


__all__ = [
    "TELEGRAM_TEXT_LIMIT",
    "cap_telegram_text",
    "escape_tg_html",
    "telegram_safe",
    "telegram_text_units",
]
