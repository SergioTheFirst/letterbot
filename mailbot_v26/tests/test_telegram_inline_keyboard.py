from __future__ import annotations

from datetime import datetime

import pytest

from mailbot_v26.pipeline.processor import MessageInterpretation
from mailbot_v26.telegram.callback_data import (
    CallbackData,
    FEEDBACK_ACTIONS,
    FEEDBACK_PREFIX,
    MAX_BYTES,
    PRIORITY_ACTIONS,
    PRIORITY_PREFIX,
    decode,
    encode,
    is_valid,
)
from mailbot_v26.telegram.render_contract import (
    TelegramRenderRequest,
    render_email_notification,
)


def _interpretation(
    *,
    email_id: str,
    sender_email: str,
    doc_kind: str | None,
    priority: str,
    action: str,
) -> MessageInterpretation:
    return MessageInterpretation(
        email_id=email_id,
        sender_email=sender_email,
        doc_kind=doc_kind,
        amount=87500.0 if doc_kind == "invoice" else None,
        due_date="2026-04-15" if doc_kind == "invoice" else None,
        action=action,
        priority=priority,
        confidence=0.92,
        context="NEW_MESSAGE",
        document_id=f"{doc_kind or 'generic'}-{email_id}",
        issuer_label="ООО Вектор",
    )


def _request(
    *,
    email_id: int,
    doc_kind: str | None,
    priority: str,
    action: str,
    llm_failed: bool = False,
    signal_invalid: bool = False,
    body_summary: str = "Краткая сводка",
    body_text: str = "Краткая сводка",
) -> TelegramRenderRequest:
    interpretation = None
    if doc_kind is not None:
        interpretation = _interpretation(
            email_id=str(email_id),
            sender_email="sender@example.com",
            doc_kind=doc_kind,
            priority=priority,
            action=action,
        )
    return TelegramRenderRequest(
        email_id=email_id,
        received_at=datetime(2024, 1, 1, 12, 0),
        sender_email="sender@example.com",
        sender_name="ООО Вектор",
        subject="Тестовое письмо",
        interpretation=interpretation,
        action_line=action,
        mail_type=(doc_kind or "").upper(),
        body_summary=body_summary,
        body_text=body_text,
        llm_failed=llm_failed,
        signal_invalid=signal_invalid,
    )


def _decode_markup(markup: dict[str, object] | None) -> list[CallbackData]:
    if not isinstance(markup, dict):
        return []
    decoded: list[CallbackData] = []
    for row in markup.get("inline_keyboard", []):
        for button in row:
            callback = button.get("callback_data")
            if isinstance(callback, str) and is_valid(callback):
                decoded.append(decode(callback))
    return decoded


def test_callback_data_encode_within_64_bytes() -> None:
    encoded = encode(prefix=FEEDBACK_PREFIX, action="paid", msg_key="123456789")

    assert len(encoded.encode("utf-8")) <= MAX_BYTES


def test_callback_data_encode_all_defined_actions() -> None:
    values = {
        encode(prefix=FEEDBACK_PREFIX, action=action, msg_key="42")
        for action in FEEDBACK_ACTIONS
    }
    values.update(
        encode(prefix=PRIORITY_PREFIX, action=action, msg_key="42")
        for action in PRIORITY_ACTIONS
    )

    assert len(values) == len(FEEDBACK_ACTIONS) + len(PRIORITY_ACTIONS)


def test_callback_data_decode_roundtrip() -> None:
    original = encode(prefix=PRIORITY_PREFIX, action="hi", msg_key="777")

    decoded = decode(original)

    assert decoded.prefix == PRIORITY_PREFIX
    assert decoded.action == "hi"
    assert decoded.msg_key == "777"


@pytest.mark.parametrize(
    "value",
    [
        "",
        "garbage",
        "FB:paid",
        "XX:paid:1",
        "FB:nope:1",
        "FB:paid:not-a-number",
    ],
)
def test_callback_data_decode_malformed_raises(value: str) -> None:
    with pytest.raises(ValueError):
        decode(value)


def test_callback_data_is_valid_true_for_valid() -> None:
    assert is_valid(encode(prefix=FEEDBACK_PREFIX, action="correct", msg_key="15"))


def test_callback_data_is_valid_false_for_garbage() -> None:
    assert is_valid("paid:15") is False


def test_callback_data_msg_key_fits_budget() -> None:
    with pytest.raises(ValueError):
        encode(prefix=FEEDBACK_PREFIX, action="paid", msg_key="9" * 32)


def test_callback_data_collision_safe_lookup_or_rejection() -> None:
    first = encode(prefix=PRIORITY_PREFIX, action="lo", msg_key="123")
    second = encode(prefix=PRIORITY_PREFIX, action="lo", msg_key="124")

    assert first != second
    with pytest.raises(ValueError):
        encode(prefix=PRIORITY_PREFIX, action="lo", msg_key="123_legacy")


def test_render_invoice_has_reply_markup() -> None:
    result = render_email_notification(
        _request(
            email_id=101,
            doc_kind="invoice",
            priority="🟡",
            action="Оплатить счёт",
        )
    )

    assert result.reply_markup is not None
    decoded = _decode_markup(result.reply_markup)
    assert {item.action for item in decoded if item.prefix == FEEDBACK_PREFIX} >= {
        "paid",
        "not_invoice",
        "snooze",
    }


def test_render_invoice_priority_buttons_preserve_low_medium_high_order() -> None:
    result = render_email_notification(
        _request(
            email_id=107,
            doc_kind="invoice",
            priority="рџџЎ",
            action="РћРїР»Р°С‚РёС‚СЊ СЃС‡С‘С‚",
        )
    )

    assert result.reply_markup is not None
    priority_row = result.reply_markup["inline_keyboard"][-2]
    assert [button["text"] for button in priority_row] == [
        "🔵 Low",
        "🟡 Medium",
        "🔴 High",
    ]
    assert [decode(button["callback_data"]).action for button in priority_row] == [
        "lo",
        "med",
        "hi",
    ]


def test_render_payroll_has_reply_markup() -> None:
    result = render_email_notification(
        _request(
            email_id=102,
            doc_kind="payroll",
            priority="🟡",
            action="Принять к сведению",
        )
    )

    assert result.reply_markup is not None
    decoded = _decode_markup(result.reply_markup)
    assert {item.action for item in decoded if item.prefix == FEEDBACK_PREFIX} >= {
        "correct",
        "not_payroll",
    }


def test_render_contract_has_reply_markup() -> None:
    result = render_email_notification(
        _request(
            email_id=103,
            doc_kind="contract",
            priority="🟡",
            action="Проверить договор",
        )
    )

    assert result.reply_markup is not None
    decoded = _decode_markup(result.reply_markup)
    assert {item.action for item in decoded if item.prefix == FEEDBACK_PREFIX} >= {
        "correct",
        "not_contract",
    }


def test_render_low_priority_has_no_reply_markup() -> None:
    result = render_email_notification(
        _request(
            email_id=104,
            doc_kind=None,
            priority="🔵",
            action="Принять к сведению",
            body_summary="",
            body_text="",
        )
    )

    assert result.reply_markup is None


def test_render_error_has_no_reply_markup() -> None:
    result = render_email_notification(
        _request(
            email_id=105,
            doc_kind=None,
            priority="🟡",
            action="Проверить письмо",
            llm_failed=True,
            signal_invalid=True,
            body_summary="",
            body_text="",
        )
    )

    assert result.render_mode == "safe_fallback"
    assert result.reply_markup is None


def test_render_markup_message_id_matches_render_context() -> None:
    result = render_email_notification(
        _request(
            email_id=106,
            doc_kind="invoice",
            priority="🟡",
            action="Оплатить счёт",
        )
    )

    decoded = _decode_markup(result.reply_markup)
    assert decoded
    assert {item.msg_key for item in decoded} == {"106"}
