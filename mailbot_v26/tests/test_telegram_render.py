from __future__ import annotations

import re
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

from mailbot_v26.pipeline.processor import MessageInterpretation
from mailbot_v26.telegram.render_contract import (
    TelegramRenderRequest,
    render_email_notification,
)
from mailbot_v26.text.mojibake import normalize_mojibake_text

_SNAPSHOT_DIR = Path("mailbot_v26/tests/fixtures/telegram_snapshots")
_BAD_MOJIBAKE_TOKENS = ("Р Р†Р вЂљ", "Р В РЎвЂўР РЋРІР‚С™", "РЎР‚РЎСџ", "вЂ")


def _interpretation(
    *,
    email_id: str,
    sender_email: str,
    doc_kind: str,
    amount: float | None,
    due_date: str | None,
    action: str,
    priority: str = "🔴",
    issuer_label: str | None = "ООО Вектор",
) -> MessageInterpretation:
    return MessageInterpretation(
        email_id=email_id,
        sender_email=sender_email,
        doc_kind=doc_kind,
        amount=amount,
        due_date=due_date,
        action=action,
        priority=priority,
        confidence=0.93,
        context="NEW_MESSAGE",
        document_id=f"{doc_kind}-{email_id}",
        issuer_label=issuer_label,
    )


def _snapshot_path(name: str) -> Path:
    return _SNAPSHOT_DIR / f"{name}.txt"


def _assert_snapshot(name: str, text: str) -> None:
    path = _snapshot_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(text, encoding="utf-8")
        return
    assert path.read_text(encoding="utf-8").rstrip("\n") == text.rstrip("\n")


def _full_request() -> TelegramRenderRequest:
    interpretation = _interpretation(
        email_id="101",
        sender_email="billing@vector.example",
        doc_kind="invoice",
        amount=87500.0,
        due_date="2026-04-15",
        action="Оплатить счёт",
    )
    return TelegramRenderRequest(
        email_id=101,
        received_at=datetime(2024, 1, 1, 12, 0),
        sender_email="billing@vector.example",
        sender_name="ООО Вектор",
        subject="Счёт на оплату №42",
        interpretation=interpretation,
        action_line="Проверить счёт",
        mail_type="INVOICE",
        body_summary="Счёт на 87 500 руб. Оплатить до 2026-04-15.",
        body_text="Счёт на 87 500 руб. Оплатить до 2026-04-15.",
    )


def _short_request() -> TelegramRenderRequest:
    return TelegramRenderRequest(
        email_id=102,
        received_at=datetime(2024, 1, 2, 9, 15),
        sender_email="info@example.com",
        subject="Короткое уведомление",
    )


def _fallback_request() -> TelegramRenderRequest:
    return TelegramRenderRequest(
        email_id=103,
        received_at=datetime(2024, 1, 3, 8, 30),
        sender_email="",
        subject="",
        llm_failed=True,
        signal_invalid=True,
    )


def _premium_request() -> TelegramRenderRequest:
    interpretation = _interpretation(
        email_id="104",
        sender_email="contracts@vector.example",
        doc_kind="contract",
        amount=None,
        due_date="2026-05-01",
        action="Проверить договор",
        priority="🟡",
        issuer_label="ООО Вектор",
    )
    return TelegramRenderRequest(
        email_id=104,
        received_at=datetime(2024, 1, 4, 10, 45),
        sender_email="contracts@vector.example",
        sender_name="ООО Вектор",
        subject="Допсоглашение к договору",
        interpretation=interpretation,
        action_line="Проверить договор",
        mail_type="CONTRACT",
        body_summary="Проверьте условия дополнительного соглашения.",
        body_text="Проверьте условия дополнительного соглашения.",
        enable_premium_clarity=True,
    )


def test_render_full_snapshot() -> None:
    result = render_email_notification(_full_request())
    assert result.render_mode == "full"
    _assert_snapshot("full", result.text)


def test_render_safe_fallback_snapshot() -> None:
    result = render_email_notification(_fallback_request())
    assert result.render_mode == "safe_fallback"
    _assert_snapshot("safe_fallback", result.text)


def test_render_short_template_snapshot() -> None:
    result = render_email_notification(_short_request())
    assert result.render_mode == "short_template"
    _assert_snapshot("short_template", result.text)


def test_render_premium_clarity_snapshot() -> None:
    result = render_email_notification(_premium_request())
    assert result.render_mode == "full"
    _assert_snapshot("premium_clarity", result.text)


def test_render_does_not_produce_mojibake_for_cyrillic_content() -> None:
    result = render_email_notification(_full_request())

    assert normalize_mojibake_text(result.text) == result.text
    for token in _BAD_MOJIBAKE_TOKENS:
        assert token not in result.text


def test_render_does_not_produce_mojibake_for_amounts_with_special_chars() -> None:
    request = _full_request()
    request = replace(
        request,
        subject="Счёт №42 & €",
        body_summary="Сумма 1 234,56 € и аванс 50 ₽.",
        body_text="Сумма 1 234,56 € и аванс 50 ₽.",
    )

    result = render_email_notification(request)

    assert normalize_mojibake_text(result.text) == result.text
    assert "&amp;" in result.text
    assert "№42" in result.text


def test_render_amount_formatted_consistently() -> None:
    interpretation = _interpretation(
        email_id="105",
        sender_email="billing@vector.example",
        doc_kind="invoice",
        amount=1234.56,
        due_date="2026-04-15",
        action="Оплатить счёт",
    )
    result = render_email_notification(
        TelegramRenderRequest(
            email_id=105,
            received_at=datetime(2024, 1, 5, 11, 0),
            sender_email="billing@vector.example",
            sender_name="ООО Вектор",
            subject="Счёт на оплату",
            interpretation=interpretation,
            body_summary="Сумма к оплате: 1 234.56 USD",
            body_text="Сумма к оплате: 1 234.56 USD",
            mail_type="INVOICE",
        )
    )

    assert "1234.5600001" not in result.text
    assert re.search(r"\b1 235\b|\b1 234\.56\b", result.text)


def test_render_date_is_iso_not_locale_dependent() -> None:
    result = render_email_notification(_full_request())

    assert result.timestamp_iso == "2024-01-01T12:00:00"


@pytest.mark.parametrize(
    "request_factory",
    [_full_request, _short_request, _fallback_request, _premium_request],
)
def test_render_empty_facts_does_not_crash_any_mode(request_factory) -> None:
    request = request_factory()
    request = replace(
        request,
        body_summary="",
        body_text="",
        attachments=[],
        attachment_summaries=[],
    )

    result = render_email_notification(request)

    assert isinstance(result.text, str)
    assert result.parse_mode == "HTML"


def test_one_message_rule_enforced_at_render_layer() -> None:
    result = render_email_notification(_full_request())

    assert "Обрабатываю вложения" not in result.text
    assert "Письмо получено" not in result.text


def test_render_result_has_required_contract_fields() -> None:
    result = render_email_notification(_full_request())

    assert result.parse_mode == "HTML"
    assert result.reply_markup is not None
    assert result.render_mode == "full"
    assert result.message_ref == "101"
    assert result.timestamp_iso == "2024-01-01T12:00:00"
    assert result.sender_identity_key
    assert result.sender_identity_label == "ООО Вектор"


def test_render_full_payload_has_no_watermark() -> None:
    result = render_email_notification(_full_request())

    assert "Powered by LetterBot.ru" not in result.text
