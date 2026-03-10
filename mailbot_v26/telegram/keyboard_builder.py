from __future__ import annotations

from mailbot_v26.telegram.callback_data import (
    FEEDBACK_PREFIX,
    PRIORITY_PREFIX,
    encode,
)
from mailbot_v26.telegram.decision_trace_ui import (
    DETAILS_PREFIX,
    HIDE_PREFIX,
    build_decision_trace_callback,
)

InlineKeyboardMarkup = dict[str, list[list[dict[str, str]]]]

_ACTIONABLE_DOC_KINDS = {"invoice", "payroll", "reconciliation", "contract"}
_LOW_PRIORITY = "🔵"


def _safe_button(*, text: str, prefix: str, action: str, message_key: str) -> dict[str, str]:
    return {
        "text": text,
        "callback_data": encode(prefix=prefix, action=action, msg_key=message_key),
    }


def _priority_row(message_key: str) -> list[dict[str, str]]:
    return [
        _safe_button(
            text="LOW",
            prefix=PRIORITY_PREFIX,
            action="lo",
            message_key=message_key,
        ),
        _safe_button(
            text="MEDIUM",
            prefix=PRIORITY_PREFIX,
            action="med",
            message_key=message_key,
        ),
        _safe_button(
            text="HIGH",
            prefix=PRIORITY_PREFIX,
            action="hi",
            message_key=message_key,
        ),
    ]


def _snooze_row(message_key: str) -> list[dict[str, str]]:
    return [
        {
            "text": "Snooze 2 часа",
            "callback_data": f"snz_s:{message_key}:2h",
        },
        {
            "text": "Завтра",
            "callback_data": f"snz_s:{message_key}:tom",
        },
    ]


def _feedback_rows(doc_kind: str, message_key: str) -> list[list[dict[str, str]]]:
    if doc_kind == "invoice":
        return [[
            _safe_button(
                text="✓ Оплачено",
                prefix=FEEDBACK_PREFIX,
                action="paid",
                message_key=message_key,
            ),
            _safe_button(
                text="✗ Не счёт",
                prefix=FEEDBACK_PREFIX,
                action="not_invoice",
                message_key=message_key,
            ),
            _safe_button(
                text="⏸ Позже",
                prefix=FEEDBACK_PREFIX,
                action="snooze",
                message_key=message_key,
            ),
        ]]
    if doc_kind == "payroll":
        return [[
            _safe_button(
                text="✓ Принято к сведению",
                prefix=FEEDBACK_PREFIX,
                action="correct",
                message_key=message_key,
            ),
            _safe_button(
                text="✗ Неверная классификация",
                prefix=FEEDBACK_PREFIX,
                action="not_payroll",
                message_key=message_key,
            ),
        ]]
    if doc_kind == "contract":
        return [[
            _safe_button(
                text="✓ На review",
                prefix=FEEDBACK_PREFIX,
                action="correct",
                message_key=message_key,
            ),
            _safe_button(
                text="✗ Не контракт",
                prefix=FEEDBACK_PREFIX,
                action="not_contract",
                message_key=message_key,
            ),
        ]]
    if doc_kind == "reconciliation":
        return [[
            _safe_button(
                text="✓ Принято к сведению",
                prefix=FEEDBACK_PREFIX,
                action="correct",
                message_key=message_key,
            ),
        ]]
    return []


def _trace_row(*, message_key: str, expanded: bool) -> list[dict[str, str]]:
    callback = build_decision_trace_callback(
        HIDE_PREFIX if expanded else DETAILS_PREFIX,
        int(message_key),
    )
    return [
        {
            "text": "◀ Скрыть" if expanded else "Почему так?",
            "callback_data": callback,
        }
    ]


def build_notification_keyboard(
    *,
    render_mode: str,
    doc_kind: str | None,
    priority: str | None,
    message_key: int | str | None,
    show_decision_trace: bool = False,
    decision_trace_expanded: bool = False,
) -> InlineKeyboardMarkup | None:
    normalized_mode = str(render_mode or "").strip().lower()
    if normalized_mode in {"safe_fallback", "short_template", "error", "startup", "degraded"}:
        return None

    try:
        normalized_key = str(int(str(message_key or "").strip()))
        _ = encode(prefix=PRIORITY_PREFIX, action="med", msg_key=normalized_key)
    except (TypeError, ValueError):
        return None

    normalized_doc_kind = str(doc_kind or "").strip().lower()
    normalized_priority = str(priority or "").strip()

    rows = _feedback_rows(normalized_doc_kind, normalized_key)
    if normalized_doc_kind in _ACTIONABLE_DOC_KINDS or normalized_mode == "full":
        rows.append(_priority_row(normalized_key))
        rows.append(_snooze_row(normalized_key))
    if show_decision_trace:
        rows.append(
            _trace_row(
                message_key=normalized_key,
                expanded=decision_trace_expanded,
            )
        )
    if not rows:
        return None
    return {"inline_keyboard": rows}


__all__ = ["InlineKeyboardMarkup", "build_notification_keyboard"]
