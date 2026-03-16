from __future__ import annotations

from typing import Any

from mailbot_v26.observability import get_logger
from mailbot_v26.ui.i18n import DEFAULT_LOCALE

DETAILS_PREFIX = "mb:d:"
HIDE_PREFIX = "mb:h:"
PRIO_MENU_PREFIX = "prio_menu:"
PRIO_SET_PREFIX = "prio_set:"
PRIO_BACK_PREFIX = "prio_back:"
SNOOZE_MENU_PREFIX = "snz_m:"
SNOOZE_SET_PREFIX = "snz_s:"
SNOOZE_BACK_PREFIX = "snz_b:"
PRIO_OK_PREFIX = "mb:ok:"

logger = get_logger("mailbot")


def _is_english_locale(locale: str | None) -> bool:
    cleaned = str(locale or "").strip() or DEFAULT_LOCALE
    return cleaned.casefold().startswith("en")


def _trace_label(*, expanded: bool, locale: str) -> str:
    if expanded:
        return "◀ Hide" if _is_english_locale(locale) else "◀ Скрыть"
    return "Why this?" if _is_english_locale(locale) else "Почему так?"


def build_decision_trace_callback(prefix: str, email_id: int) -> str:
    return f"{prefix}{int(email_id)}"


def assert_callback_data_safe(value: str) -> None:
    if not isinstance(value, str):
        raise ValueError("callback_data must be a string")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("callback_data must be ASCII") from exc
    if not value:
        raise ValueError("callback_data must be non-empty")
    if len(value.encode("ascii")) > 64:
        raise ValueError("callback_data exceeds 64 bytes")


def _safe_callback(value: str) -> str:
    assert_callback_data_safe(value)
    return value


def _fallback_keyboard(
    *, email_id: int, expanded: bool, locale: str = DEFAULT_LOCALE
) -> dict[str, Any]:
    label = _trace_label(expanded=expanded, locale=locale)
    callback = build_decision_trace_callback(
        HIDE_PREFIX if expanded else DETAILS_PREFIX, email_id
    )
    return {"inline_keyboard": [[{"text": label, "callback_data": callback}]]}


def _priority_row(
    email_id: int, *, locale: str = DEFAULT_LOCALE
) -> list[dict[str, str]]:
    del locale
    return [
        {
            "text": "🟦▌Low",
            "callback_data": _safe_callback(f"{PRIO_SET_PREFIX}{email_id}:B"),
        },
        {
            "text": "🟨▌Medium",
            "callback_data": _safe_callback(f"{PRIO_SET_PREFIX}{email_id}:Y"),
        },
        {
            "text": "🟥▌High",
            "callback_data": _safe_callback(f"{PRIO_SET_PREFIX}{email_id}:R"),
        },
    ]


def _snooze_row(
    email_id: int, *, locale: str = DEFAULT_LOCALE
) -> list[dict[str, str]]:
    return [
        {
            "text": "Snooze 2h" if _is_english_locale(locale) else "Отложить на 2 часа",
            "callback_data": _safe_callback(f"{SNOOZE_SET_PREFIX}{email_id}:2h"),
        },
        {
            "text": "Tomorrow" if _is_english_locale(locale) else "Завтра",
            "callback_data": _safe_callback(f"{SNOOZE_SET_PREFIX}{email_id}:tom"),
        },
    ]


def build_decision_trace_keyboard(
    *,
    email_id: int,
    expanded: bool,
    locale: str = DEFAULT_LOCALE,
) -> dict[str, Any]:
    if expanded:
        callback = build_decision_trace_callback(HIDE_PREFIX, email_id)
    else:
        callback = build_decision_trace_callback(DETAILS_PREFIX, email_id)
    label = _trace_label(expanded=expanded, locale=locale)
    return {"inline_keyboard": [[{"text": label, "callback_data": callback}]]}


def build_email_actions_keyboard(
    *,
    email_id: int | str,
    expanded: bool,
    prio_menu: bool = False,
    snooze_menu: bool = False,
    initial_prio: bool = False,
    show_decision_trace: bool = False,
    locale: str = DEFAULT_LOCALE,
) -> dict[str, Any]:
    del expanded, initial_prio, show_decision_trace
    email_id_int = int(str(email_id))
    try:
        if prio_menu:
            return {"inline_keyboard": [_priority_row(email_id_int, locale=locale)]}

        if snooze_menu:
            return {"inline_keyboard": [_snooze_row(email_id_int, locale=locale)]}

        return {
            "inline_keyboard": [
                _priority_row(email_id_int, locale=locale),
                _snooze_row(email_id_int, locale=locale),
            ]
        }
    except ValueError as exc:
        logger.error("telegram_keyboard_callback_invalid", error=str(exc))
        return _fallback_keyboard(email_id=email_id_int, expanded=False, locale=locale)


__all__ = [
    "DETAILS_PREFIX",
    "HIDE_PREFIX",
    "build_decision_trace_keyboard",
    "build_decision_trace_callback",
    "build_email_actions_keyboard",
    "assert_callback_data_safe",
    "PRIO_MENU_PREFIX",
    "PRIO_SET_PREFIX",
    "PRIO_BACK_PREFIX",
    "SNOOZE_MENU_PREFIX",
    "SNOOZE_SET_PREFIX",
    "SNOOZE_BACK_PREFIX",
    "PRIO_OK_PREFIX",
]
