from __future__ import annotations

InlineKeyboardMarkup = dict[str, list[list[dict[str, str]]]]


def build_priority_keyboard(email_id: int | str) -> InlineKeyboardMarkup:
    normalized_id = str(email_id).strip()
    return {
        "inline_keyboard": [
            [
                {"text": "🔴 Срочно", "callback_data": f"mb:prio:{normalized_id}:R"},
                {"text": "🟡 Важно", "callback_data": f"mb:prio:{normalized_id}:Y"},
                {"text": "🔵 Низкий", "callback_data": f"mb:prio:{normalized_id}:B"},
            ]
        ]
    }


__all__ = ["InlineKeyboardMarkup", "build_priority_keyboard"]
