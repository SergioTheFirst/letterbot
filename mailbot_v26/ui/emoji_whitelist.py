from __future__ import annotations

import re

# CONSTITUTION Appendix A: the closed emoji list for Telegram output.
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

# Same code points as ⏸️/⚠️ without the emoji presentation selector.
_TEXT_PRESENTATION_FORMS: tuple[str, ...] = ("⏸", "⚠")

_EMOJI_RANGES = (
    (0x2300, 0x23FF),  # misc technical (⌚ ⌛ ⏩ ...)
    (0x25FD, 0x25FE),  # ◽ ◾ (emoji presentation by default)
    (0x2600, 0x26FF),  # misc symbols
    (0x2700, 0x27BF),  # dingbats (✓ ✗ ✅ ...)
    (0x2B00, 0x2BFF),  # misc symbols and arrows (⬆ ⭐ ⬛ ...)
    (0x1F000, 0x1FAFF),  # mahjong .. pictographs extended-A, incl. 🟥 flags
)

_PLANE_CLASS = "".join(f"\\U{start:08x}-\\U{end:08x}" for start, end in _EMOJI_RANGES)
# Emoji presentation selector, combining keycap, flag tag characters.
_MARK_CLASS = "\\ufe0f\\u20e3\\U000e0020-\\U000e007f"
_ALLOWED_ALT = "|".join(
    re.escape(emoji)
    for emoji in sorted(
        ALLOWED_EMOJIS + _TEXT_PRESENTATION_FORMS, key=len, reverse=True
    )
)
# One emoji: a pictograph with its marks, or a text symbol (digit, ©, ℹ, ↩ ...)
# forced into emoji presentation by U+FE0F. Never swallows an allowed emoji.
_ELEMENT = (
    f"(?!{_ALLOWED_ALT})"
    f"(?:[{_PLANE_CLASS}][{_MARK_CLASS}]*"
    "|[#*0-9\\u00a9\\u00ae\\u203c-\\u3299]\\ufe0f\\u20e3?)"
)
_SEQUENCE = f"{_ELEMENT}(?:\\u200d{_ELEMENT})*"
_ORPHAN_MARK = f"[{_MARK_CLASS}]"

_CANDIDATE_RE = re.compile(f"[{_PLANE_CLASS}{_MARK_CLASS}]")
_FIND_RE = re.compile(f"(?P<allowed>{_ALLOWED_ALT})|(?P<bad>{_SEQUENCE}|{_ORPHAN_MARK})")
# An off-list run at a line/tag start also takes the gap after it, so
# "📊 Week" becomes "Week" rather than " Week".
_STRIP_RE = re.compile(
    f"(?P<allowed>{_ALLOWED_ALT})"
    f"|(?P<lead>(?<![^\\s>])(?:{_SEQUENCE})+[ \\t]*)"
    f"|(?P<bad>(?:{_SEQUENCE})+|{_ORPHAN_MARK})"
)


def _keep_allowed(match: re.Match[str]) -> str:
    return match.group("allowed") or ""


def strip_disallowed_emojis(text: str) -> str:
    if not text:
        return ""
    if not _CANDIDATE_RE.search(text):
        return text
    return _STRIP_RE.sub(_keep_allowed, text)


def find_disallowed_emojis(text: str) -> set[str]:
    if not text or not _CANDIDATE_RE.search(text):
        return set()
    return {
        match.group("bad")
        for match in _FIND_RE.finditer(text)
        if match.group("bad")
    }


def allowed_emojis_pattern() -> re.Pattern[str]:
    escaped = "|".join(re.escape(emoji) for emoji in ALLOWED_EMOJIS)
    return re.compile(escaped)


__all__ = [
    "ALLOWED_EMOJIS",
    "allowed_emojis_pattern",
    "find_disallowed_emojis",
    "strip_disallowed_emojis",
]
