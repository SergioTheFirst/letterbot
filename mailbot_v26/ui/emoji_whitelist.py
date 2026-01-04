from __future__ import annotations

import re

ALLOWED_EMOJIS: tuple[str, ...] = (
    "🔴",
    "🟡",
    "🔵",
    "⚡",
    "💬",
    "⏸️",
    "📎",
    "💰",
    "📄",
    "⏰",
    "⚠️",
    "📦",
    "📧",
)

_EMOJI_RANGES = (
    (0x2300, 0x23FF),
    (0x1F300, 0x1F5FF),
    (0x1F600, 0x1F64F),
    (0x1F680, 0x1F6FF),
    (0x2600, 0x26FF),
    (0x2700, 0x27BF),
    (0x1F900, 0x1F9FF),
    (0x1FA70, 0x1FAFF),
)

_PLACEHOLDER_PREFIX = "__ALLOWED_EMOJI_"


def _is_emoji_char(char: str) -> bool:
    code = ord(char)
    return any(start <= code <= end for start, end in _EMOJI_RANGES)


def strip_disallowed_emojis(text: str) -> str:
    if not text:
        return ""
    placeholders: dict[str, str] = {}
    replaced = text
    for index, emoji in enumerate(ALLOWED_EMOJIS):
        placeholder = f"{_PLACEHOLDER_PREFIX}{index}__"
        if emoji in replaced:
            placeholders[placeholder] = emoji
            replaced = replaced.replace(emoji, placeholder)

    cleaned_chars = [char for char in replaced if not _is_emoji_char(char)]
    cleaned = "".join(cleaned_chars)
    for placeholder, emoji in placeholders.items():
        cleaned = cleaned.replace(placeholder, emoji)
    return cleaned


def find_disallowed_emojis(text: str) -> set[str]:
    if not text:
        return set()
    disallowed: set[str] = set()
    masked = text
    for emoji in ALLOWED_EMOJIS:
        masked = masked.replace(emoji, "")
    for char in masked:
        if _is_emoji_char(char):
            disallowed.add(char)
    return disallowed


def allowed_emojis_pattern() -> re.Pattern[str]:
    escaped = "|".join(re.escape(emoji) for emoji in ALLOWED_EMOJIS)
    return re.compile(escaped)


__all__ = [
    "ALLOWED_EMOJIS",
    "allowed_emojis_pattern",
    "find_disallowed_emojis",
    "strip_disallowed_emojis",
]
