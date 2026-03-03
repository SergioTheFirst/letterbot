from __future__ import annotations

from typing import Any

from mailbot_v26.observability import get_logger

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


def _fallback_keyboard(*, email_id: int, expanded: bool) -> dict[str, Any]:
    label = "◀ Скрыть" if expanded else "Почему так?"
    callback = build_decision_trace_callback(HIDE_PREFIX if expanded else DETAILS_PREFIX, email_id)
    return {"inline_keyboard": [[{"text": label, "callback_data": callback}]]}


def build_decision_trace_keyboard(*, email_id: int, expanded: bool) -> dict[str, Any]:
    if expanded:
        label = "◀ Скрыть"
        callback = build_decision_trace_callback(HIDE_PREFIX, email_id)
    else:
        label = "Почему так?"
        callback = build_decision_trace_callback(DETAILS_PREFIX, email_id)
    return {"inline_keyboard": [[{"text": label, "callback_data": callback}]]}


def build_email_actions_keyboard(
    *,
    email_id: int | str,
    expanded: bool,
    prio_menu: bool = False,
    snooze_menu: bool = False,
    initial_prio: bool = True,
    show_decision_trace: bool = False,
) -> dict[str, Any]:
    email_id_int = int(str(email_id))
    try:
        if prio_menu:
            priority_row = [
                {
                    "text": "🔴 Срочно",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:R"
                    ),
                },
                {
                    "text": "🟡 Важно",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:Y"
                    ),
                },
                {
                    "text": "🔵 Низкий",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:B"
                    ),
                },
            ]
            back_row = [
                {
                    "text": "Назад",
                    "callback_data": _safe_callback(f"{PRIO_BACK_PREFIX}{email_id_int}"),
                }
            ]
            return {"inline_keyboard": [priority_row, back_row]}

        if snooze_menu:
            snooze_row = [
                {"text": "2ч", "callback_data": _safe_callback(f"{SNOOZE_SET_PREFIX}{email_id_int}:2h")},
                {"text": "6ч", "callback_data": _safe_callback(f"{SNOOZE_SET_PREFIX}{email_id_int}:6h")},
                {"text": "Завтра", "callback_data": _safe_callback(f"{SNOOZE_SET_PREFIX}{email_id_int}:tom")},
            ]
            back_row = [
                {
                    "text": "Назад",
                    "callback_data": _safe_callback(f"{SNOOZE_BACK_PREFIX}{email_id_int}"),
                }
            ]
            return {"inline_keyboard": [snooze_row, back_row]}

        if initial_prio and not prio_menu and not snooze_menu:
            priority_row = [
                {
                    "text": "🔴 Срочно",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:R"
                    ),
                },
                {
                    "text": "🟡 Важно",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:Y"
                    ),
                },
                {
                    "text": "🔵 Низкий",
                    "callback_data": _safe_callback(
                        f"{PRIO_SET_PREFIX}{email_id_int}:B"
                    ),
                },
            ]
            return {"inline_keyboard": [priority_row]}

        prio_callback = _safe_callback(f"{PRIO_MENU_PREFIX}{email_id_int}")
        snooze_callback = _safe_callback(f"{SNOOZE_MENU_PREFIX}{email_id_int}")
        ok_callback = _safe_callback(f"{PRIO_OK_PREFIX}{email_id_int}")
        first_row: list[dict[str, str]] = []
        if show_decision_trace:
            trace_label = "◀ Скрыть" if expanded else "Почему так?"
            trace_callback = _safe_callback(
                build_decision_trace_callback(
                    HIDE_PREFIX if expanded else DETAILS_PREFIX, email_id_int
                )
            )
            first_row.append({"text": trace_label, "callback_data": trace_callback})
        first_row.extend(
            [
                {"text": "Приоритет", "callback_data": prio_callback},
                {"text": "⏰ Позже", "callback_data": snooze_callback},
            ]
        )
        return {
            "inline_keyboard": [
                first_row,
                [
                    {"text": "✓ Верно", "callback_data": ok_callback},
                ],
            ]
        }
    except ValueError as exc:
        logger.error("telegram_keyboard_callback_invalid", error=str(exc))
        return _fallback_keyboard(email_id=email_id_int, expanded=expanded)


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
