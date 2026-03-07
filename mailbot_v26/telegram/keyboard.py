from __future__ import annotations

InlineKeyboardMarkup = dict[str, list[list[dict[str, str]]]]


def build_priority_keyboard(email_id: int | str) -> InlineKeyboardMarkup:
    normalized_id = str(email_id).strip()
    return {
        "inline_keyboard": [
            [
                {"text": "Изменить приоритет", "callback_data": f"prio_menu:{normalized_id}"},
                {"text": "⏰ Отложить", "callback_data": f"snz_m:{normalized_id}"},
            ]
        ]
    }


__all__ = ["InlineKeyboardMarkup", "build_priority_keyboard"]
