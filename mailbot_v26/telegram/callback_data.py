from __future__ import annotations

from dataclasses import dataclass
import re

MAX_BYTES = 64
_MSG_KEY_RE = re.compile(r"^\d{1,24}$")

FEEDBACK_PREFIX = "FB"
PRIORITY_PREFIX = "PR"

FEEDBACK_ACTIONS = frozenset(
    {
        "paid",
        "not_invoice",
        "not_payroll",
        "not_contract",
        "correct",
        "snooze",
    }
)
PRIORITY_ACTIONS = frozenset({"hi", "med", "lo"})
_PREFIX_ACTIONS = {
    FEEDBACK_PREFIX: FEEDBACK_ACTIONS,
    PRIORITY_PREFIX: PRIORITY_ACTIONS,
}


@dataclass(frozen=True, slots=True)
class CallbackData:
    prefix: str
    action: str
    msg_key: str


def _validate_prefix_action(prefix: str, action: str) -> None:
    allowed_actions = _PREFIX_ACTIONS.get(prefix)
    if allowed_actions is None:
        raise ValueError(f"unsupported callback prefix: {prefix}")
    if action not in allowed_actions:
        raise ValueError(f"unsupported callback action: {prefix}:{action}")


def _validate_msg_key(msg_key: str) -> str:
    normalized = str(msg_key or "").strip()
    if not _MSG_KEY_RE.fullmatch(normalized):
        raise ValueError("callback msg_key must be a numeric canonical id")
    return normalized


def encode(*, prefix: str, action: str, msg_key: str) -> str:
    normalized_prefix = str(prefix or "").strip().upper()
    normalized_action = str(action or "").strip().lower()
    normalized_key = _validate_msg_key(msg_key)
    _validate_prefix_action(normalized_prefix, normalized_action)
    value = f"{normalized_prefix}:{normalized_action}:{normalized_key}"
    if len(value.encode("utf-8")) > MAX_BYTES:
        raise ValueError("callback_data exceeds Telegram 64-byte limit")
    return value


def decode(value: str) -> CallbackData:
    raw = str(value or "").strip()
    parts = raw.split(":")
    if len(parts) != 3:
        raise ValueError("callback_data must have exactly 3 parts")
    prefix, action, msg_key = parts
    normalized_prefix = prefix.strip().upper()
    normalized_action = action.strip().lower()
    normalized_key = _validate_msg_key(msg_key)
    _validate_prefix_action(normalized_prefix, normalized_action)
    if len(raw.encode("utf-8")) > MAX_BYTES:
        raise ValueError("callback_data exceeds Telegram 64-byte limit")
    return CallbackData(
        prefix=normalized_prefix,
        action=normalized_action,
        msg_key=normalized_key,
    )


def is_valid(value: str) -> bool:
    try:
        decode(value)
    except ValueError:
        return False
    return True


__all__ = [
    "CallbackData",
    "FEEDBACK_ACTIONS",
    "FEEDBACK_PREFIX",
    "MAX_BYTES",
    "PRIORITY_ACTIONS",
    "PRIORITY_PREFIX",
    "decode",
    "encode",
    "is_valid",
]
