from __future__ import annotations

import pytest

from mailbot_v26.telegram.decision_trace_ui import (
    assert_callback_data_safe,
    build_email_actions_keyboard,
)
from mailbot_v26.telegram.inbound import parse_callback_data
from mailbot_v26.telegram.keyboard import build_priority_keyboard


def test_priority_keyboard_callback_data_length() -> None:
    keyboard = build_email_actions_keyboard(email_id=42, expanded=False, prio_menu=False, initial_prio=False)
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


def test_email_actions_keyboard_contains_snooze_button() -> None:
    keyboard = build_email_actions_keyboard(email_id=123, expanded=False, initial_prio=False)
    labels = [button["text"] for button in keyboard["inline_keyboard"][0]]
    assert labels == ["Изменить приоритет", "⏰ Отложить"]
    assert len(keyboard["inline_keyboard"]) == 1

def test_snooze_callbacks_parse() -> None:
    assert parse_callback_data("snz_m:123") == ("snooze_menu", {"email_id": "123"})
    assert parse_callback_data("snz_b:123") == ("snooze_back", {"email_id": "123"})
    assert parse_callback_data("snz_s:123:2h") == (
        "snooze_set",
        {"email_id": "123", "snooze": "2h"},
    )


def test_email_actions_keyboard_shows_trace_when_enabled() -> None:
    keyboard = build_email_actions_keyboard(email_id=123, expanded=False, initial_prio=False, show_decision_trace=True)
    labels = [button["text"] for button in keyboard["inline_keyboard"][0]]
    assert labels == ["Почему так?", "Изменить приоритет", "⏰ Отложить"]


def test_priority_menu_labels_match_user_facing_ux() -> None:
    keyboard = build_email_actions_keyboard(email_id=555, expanded=False, prio_menu=True)
    assert [[button["text"] for button in row] for row in keyboard["inline_keyboard"]] == [
        ["🔴 Срочно", "🟡 Важно", "🔵 Низкий"],
        ["↩ Назад"],
    ]


def test_initial_keyboard_shows_priority_buttons() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, initial_prio=True)
    assert [[button["text"] for button in row] for row in keyboard["inline_keyboard"]] == [[
        "🔴 Срочно", "🟡 Важно", "🔵 Низкий"
    ]]


def test_initial_keyboard_no_back_button() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, initial_prio=True)
    assert len(keyboard["inline_keyboard"]) == 1
    all_labels = [button["text"] for row in keyboard["inline_keyboard"] for button in row]
    assert "Назад" not in all_labels


def test_prio_menu_keeps_back_button() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, prio_menu=True)
    assert keyboard["inline_keyboard"][1][0]["text"] == "↩ Назад"

def test_build_priority_keyboard_has_menu_callbacks() -> None:
    keyboard = build_priority_keyboard(123)
    callback_data = [
        button["callback_data"]
        for row in keyboard.get("inline_keyboard", [])
        for button in row
    ]
    assert "prio_menu:123" in callback_data
    assert "snz_m:123" in callback_data


def test_priority_callbacks_always_include_email_id() -> None:
    email_id = 777
    for keyboard in (
        build_email_actions_keyboard(email_id=email_id, expanded=False, initial_prio=True),
        build_email_actions_keyboard(email_id=email_id, expanded=False, prio_menu=True),
    ):
        callback_data = [
            button.get("callback_data", "")
            for row in keyboard.get("inline_keyboard", [])
            for button in row
            if button.get("text") in {"🔴 Срочно", "🟡 Важно", "🔵 Низкий"}
        ]
        assert callback_data
        assert all(f":{email_id}:" in item for item in callback_data)


def test_priority_menu_buttons_are_human_readable() -> None:
    keyboard = build_email_actions_keyboard(email_id=314, expanded=False, initial_prio=False)
    labels = [button["text"] for button in keyboard["inline_keyboard"][0]]
    assert labels == ["Изменить приоритет", "⏰ Отложить"]


def test_priority_menu_does_not_show_confusing_verno_label() -> None:
    keyboard = build_email_actions_keyboard(email_id=314, expanded=False, initial_prio=False)
    labels = [button["text"] for row in keyboard["inline_keyboard"] for button in row]
    assert "✓ Верно" not in labels


def test_back_button_returns_to_main_keyboard() -> None:
    menu = build_email_actions_keyboard(email_id=314, expanded=False, prio_menu=True)
    assert menu["inline_keyboard"][1][0]["text"] == "↩ Назад"

    base = build_email_actions_keyboard(email_id=314, expanded=False, initial_prio=False)
    labels = [button["text"] for button in base["inline_keyboard"][0]]
    assert labels == ["Изменить приоритет", "⏰ Отложить"]



def test_priority_keyboard_renders_direct_priority_buttons() -> None:
    keyboard = build_email_actions_keyboard(email_id=1, expanded=False, initial_prio=True)
    assert [[button["text"] for button in row] for row in keyboard["inline_keyboard"]] == [[
        "🔴 Срочно", "🟡 Важно", "🔵 Низкий"
    ]]
