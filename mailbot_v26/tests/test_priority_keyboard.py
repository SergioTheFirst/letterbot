from __future__ import annotations

import pytest

from mailbot_v26.telegram.decision_trace_ui import (
    assert_callback_data_safe,
    build_email_actions_keyboard,
)
from mailbot_v26.telegram.inbound import parse_callback_data


def test_priority_keyboard_callback_data_length() -> None:
    keyboard = build_email_actions_keyboard(email_id=42, expanded=False, prio_menu=False)
    for row in keyboard.get("inline_keyboard", []):
        for button in row:
            callback_data = button.get("callback_data")
            assert callback_data is not None
            assert len(callback_data.encode("ascii")) <= 64

    menu = build_email_actions_keyboard(email_id=987654321, expanded=True, prio_menu=True)
    for row in menu.get("inline_keyboard", []):
        for button in row:
            callback_data = button.get("callback_data")
            assert callback_data is not None
            assert len(callback_data.encode("ascii")) <= 64


def test_callback_data_validation_rejects_non_ascii() -> None:
    with pytest.raises(ValueError):
        assert_callback_data_safe("prio_set:1:🔴")


def test_priority_menu_callbacks_parse() -> None:
    assert parse_callback_data("prio_menu:123") == ("prio_menu", {"email_id": "123"})
    assert parse_callback_data("prio_back:123") == ("prio_back", {"email_id": "123"})
    assert parse_callback_data("prio_set:123:R") == (
        "prio_set",
        {"email_id": "123", "priority": "🔴"},
    )
