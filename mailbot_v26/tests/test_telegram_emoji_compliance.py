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


# --- send boundary: send_telegram / edit_telegram_message / inbound reply ---


class _OkResponse:
    status_code = 200
    text = '{"ok": true}'

    def json(self) -> dict[str, object]:
        return {"ok": True, "result": {"message_id": 7}}


class _ParseErrorResponse:
    status_code = 400
    text = "Bad Request: can't parse entities"

    def json(self) -> dict[str, object]:
        return {"ok": False}


def _markup_with_off_list_buttons() -> dict[str, object]:
    return {
        "inline_keyboard": [
            [
                {"text": "🟥▌High", "callback_data": "prio_set:1:R"},
                {"text": "🔴 High", "callback_data": "prio_set:1:R"},
            ],
            [{"text": "✓ Paid", "callback_data": "mb:fb:paid:1"}],
        ]
    }


def _button_texts(markup: dict[str, object]) -> list[str]:
    return [button["text"] for row in markup["inline_keyboard"] for button in row]


def _capture_posts(monkeypatch, responses: list[object] | None = None) -> list[dict]:
    from mailbot_v26.worker import telegram_sender

    calls: list[dict[str, object]] = []

    def _post(**kwargs):
        calls.append(kwargs)
        return responses.pop(0) if responses else _OkResponse()

    monkeypatch.setattr(telegram_sender, "_post_message", _post)
    return calls


def _payload(text: str, markup: dict[str, object] | None = None):
    from mailbot_v26.pipeline.telegram_payload import TelegramPayload

    return TelegramPayload(
        html_text=text,
        priority="🔵",
        metadata={"bot_token": "token", "chat_id": "chat"},
        reply_markup=markup,
    )


def test_send_telegram_strips_off_list_emoji_from_text_and_buttons(
    monkeypatch, caplog
) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = _markup_with_off_list_buttons()
    caplog.set_level("WARNING")

    result = telegram_sender.send_telegram(_payload("🔴 Счёт 🚀\n📊 Итоги", markup))

    assert result.delivered is True
    assert calls[0]["text"] == "🔴 Счёт \nИтоги"
    assert _button_texts(calls[0]["reply_markup"]) == ["▌High", "🔴 High", "Paid"]
    first_button = calls[0]["reply_markup"]["inline_keyboard"][0][0]
    assert first_button["callback_data"] == "prio_set:1:R"
    # The caller's markup is not mutated.
    assert _button_texts(markup) == ["🟥▌High", "🔴 High", "✓ Paid"]
    assert "telegram_emoji_stripped" in caplog.text


def test_send_telegram_keeps_clean_payload_untouched(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = {"inline_keyboard": [[{"text": "🔴 High", "callback_data": "x"}]]}

    telegram_sender.send_telegram(_payload("🔴💰 Счёт 📎", markup))

    assert calls[0]["text"] == "🔴💰 Счёт 📎"
    assert calls[0]["reply_markup"] is markup


def test_send_telegram_plain_salvage_keeps_the_guard(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch, [_ParseErrorResponse(), _OkResponse()])

    result = telegram_sender.send_telegram(
        _payload("<b>📈 Отчёт</b>", _markup_with_off_list_buttons())
    )

    assert result.mode == "plain_salvage"
    assert calls[1]["text"] == "Отчёт"
    labels = " ".join(_button_texts(calls[1]["reply_markup"]))
    assert find_disallowed_emojis(labels) == set()


def test_edit_telegram_message_strips_off_list_emoji(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    posted: list[dict[str, object]] = []

    def _post(_url, json=None, timeout=None):
        posted.append(json)
        return _OkResponse()

    monkeypatch.setattr(telegram_sender.requests, "post", _post)

    applied = telegram_sender.edit_telegram_message(
        bot_token="token",
        chat_id="chat",
        message_id=5,
        html_text="🟡 Обновлено 🔁",
        reply_markup=_markup_with_off_list_buttons(),
    )

    assert applied is True
    assert posted[0]["text"] == "🟡 Обновлено "
    labels = " ".join(_button_texts(posted[0]["reply_markup"]))
    assert find_disallowed_emojis(labels) == set()


def test_inbound_send_message_goes_through_the_guard(monkeypatch) -> None:
    from mailbot_v26.telegram.inbound import TelegramInboundClient

    calls = _capture_posts(monkeypatch)
    client = TelegramInboundClient(bot_token="token", _requests=object())

    result = client.send_message(chat_id="chat", text="📌 Обязательства 🎯")

    assert result.delivered is True
    assert find_disallowed_emojis(calls[0]["text"]) == set()
    assert "Обязательства" in calls[0]["text"]


def test_callback_toast_goes_through_the_guard(monkeypatch) -> None:
    from types import SimpleNamespace

    from mailbot_v26.telegram.inbound import TelegramInboundProcessor
    from mailbot_v26.worker import telegram_sender

    posted: list[dict[str, object]] = []

    def _post(_url, json=None, timeout=None):
        posted.append(json)
        return _OkResponse()

    monkeypatch.setattr(telegram_sender.requests, "post", _post)

    TelegramInboundProcessor._answer_callback(
        SimpleNamespace(bot_token="token"), "cb-1", "✓ Приоритет 🟥 → 🔴"
    )

    assert posted[0]["text"] == "Приоритет → 🔴"


def test_button_made_only_of_off_list_emoji_keeps_a_label(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = {"inline_keyboard": [[{"text": "🚀", "callback_data": "x"}]]}

    telegram_sender.send_telegram(_payload("ok", markup))

    assert _button_texts(calls[0]["reply_markup"]) == ["•"]
