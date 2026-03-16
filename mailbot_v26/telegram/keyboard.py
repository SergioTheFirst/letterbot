from __future__ import annotations

from mailbot_v26.telegram.decision_trace_ui import build_email_actions_keyboard
from mailbot_v26.ui.i18n import DEFAULT_LOCALE

InlineKeyboardMarkup = dict[str, list[list[dict[str, str]]]]


def build_priority_keyboard(
    email_id: int | str, *, locale: str = DEFAULT_LOCALE
) -> InlineKeyboardMarkup:
    return build_email_actions_keyboard(
        email_id=email_id,
        expanded=False,
        initial_prio=True,
        locale=locale,
    )


__all__ = ["InlineKeyboardMarkup", "build_priority_keyboard"]
