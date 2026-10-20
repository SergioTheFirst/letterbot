"""WP7a: CONSTITUTION Appendix A emoji closed list (decision D2).

Only 🔴🟡🔵 ⚡💬⏸️ 📎 💰📄⏰⚠️📦📧 may reach Telegram. The whitelist must catch
every pictographic block, the send boundary strips anything else from text
and button labels, and every Telegram-producing builder is clean at source.
"""

from __future__ import annotations

import pytest

from mailbot_v26.ui.emoji_whitelist import (
    ALLOWED_EMOJIS,
    find_disallowed_emojis,
    strip_disallowed_emojis,
)

APPENDIX_A = "🔴🟡🔵⚡💬⏸️📎💰📄⏰⚠️📦📧"

OFF_LIST_SAMPLES = [
    "🟥",  # U+1F7E5 geometric shapes extended
    "🟨",
    "🟦",
    "🟢",
    "🟠",
    "🔹",  # dropped from the whitelist (D2)
    "🀄",  # U+1F004 mahjong
    "🃏",  # U+1F0CF playing cards
    "🆗",  # U+1F197 enclosed alphanumeric supplement
    "🈯",  # U+1F22F enclosed ideographic supplement
    "🇷🇺",  # regional indicator flag
    "⬆️",  # U+2B06 misc symbols and arrows
    "⭐",  # U+2B50
    "⬛",  # U+2B1B
    "ℹ️",  # text symbol forced to emoji presentation
    "↩️",
    "©️",
    "1️⃣",  # keycap sequence
    "#️⃣",
    "👍🏽",  # skin tone modifier
    "👨‍💻",  # ZWJ sequence
    "🏴󠁧󠁢󠁥󠁮󠁧󠁿",  # tag sequence
    "✓",  # dingbats
    "✗",
    "📊",
    "📈",
    "📌",
    "🚨",
    "💡",
    "🎯",
    "🧠",  # U+1F9E0 supplemental symbols
    "🫠",  # U+1FAE0 symbols and pictographs extended-A
]

_LEFTOVER_MARKS = ("️", "⃣", "‍")


def test_appendix_a_is_the_whole_whitelist() -> None:
    assert "".join(ALLOWED_EMOJIS) == APPENDIX_A
    assert "🔹" not in ALLOWED_EMOJIS


def test_appendix_a_emojis_survive_strip() -> None:
    text = f"Приоритет {APPENDIX_A} и ⚠ ⏸ без VS16"

    assert strip_disallowed_emojis(text) == text
    assert find_disallowed_emojis(text) == set()


@pytest.mark.parametrize("emoji", OFF_LIST_SAMPLES)
def test_off_list_emoji_is_detected_and_stripped(emoji: str) -> None:
    text = f"Счёт {emoji} оплачен"

    cleaned = strip_disallowed_emojis(text)

    assert find_disallowed_emojis(text), emoji
    assert find_disallowed_emojis(cleaned) == set()
    assert cleaned == "Счёт оплачен"
    assert not any(mark in cleaned for mark in _LEFTOVER_MARKS)
    assert not any(0xE0020 <= ord(char) <= 0xE007F for char in cleaned)


def test_plain_text_symbols_are_kept() -> None:
    text = "Счёт №42 → оплатить · ©2026 ™ ◀ Скрыть ▌ «ок» — 5×3 • пункт"

    assert strip_disallowed_emojis(text) == text
    assert find_disallowed_emojis(text) == set()


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("📊 Weekly", "Weekly"),
        ("<b>📊 Title</b>", "<b>Title</b>"),
        ("a 🚀 b", "a b"),
        ("x🚀y", "xy"),
        ("Line\n📊 Next", "Line\nNext"),
        ("🔴🚀 Срочно", "🔴 Срочно"),
        ("🚀🔴 Срочно", "🔴 Срочно"),
        ("🔴️ ok", "🔴 ok"),
        ("✓ Paid", "Paid"),
    ],
)
def test_strip_removes_the_gap_left_by_a_leading_emoji(text: str, expected: str) -> None:
    assert strip_disallowed_emojis(text) == expected


def test_find_reports_each_off_list_sequence() -> None:
    assert find_disallowed_emojis("ok 🚀⌛ 👨‍💻 🔴") == {"🚀", "⌛", "👨‍💻"}
