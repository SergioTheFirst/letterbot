from __future__ import annotations

from typing import Any

DETAILS_PREFIX = "mb:d:"
HIDE_PREFIX = "mb:h:"


def build_decision_trace_callback(prefix: str, email_id: int) -> str:
    return f"{prefix}{int(email_id)}"


def build_decision_trace_keyboard(*, email_id: int, expanded: bool) -> dict[str, Any]:
    if expanded:
        label = "◀ Скрыть"
        callback = build_decision_trace_callback(HIDE_PREFIX, email_id)
    else:
        label = "▶ Подробнее"
        callback = build_decision_trace_callback(DETAILS_PREFIX, email_id)
    return {"inline_keyboard": [[{"text": label, "callback_data": callback}]]}


__all__ = [
    "DETAILS_PREFIX",
    "HIDE_PREFIX",
    "build_decision_trace_keyboard",
    "build_decision_trace_callback",
]
