"""Telegram 4096 UTF-16 text cap (WP1b): an over-long text must still go out.

Telegram rejects sendMessage/editMessageText above 4096 UTF-16 code units
with a non-retryable 400, so the notification was lost. The cap keeps whole
lines, cuts an oversized line as plain text, never leaves an unclosed tag or a
half entity, and marks the cut with an ellipsis.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from types import SimpleNamespace

import pytest

from mailbot_v26.bot_core import pipeline as core_pipeline
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.telegram_utils import (
    TELEGRAM_TEXT_LIMIT,
    cap_telegram_text,
    telegram_text_units,
)
from mailbot_v26.worker import telegram_sender

_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9-]*)[^>]*>")
_SENTENCE = "Отгрузка партии номер сорок два задерживается на складе поставщика. "


def _units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _tags_balanced(text: str) -> bool:
    stack: list[str] = []
    for match in _TAG_RE.finditer(text):
        name = match.group(2).lower()
        if not match.group(1):
            stack.append(name)
        elif not stack or stack.pop() != name:
            return False
    return not stack and "<" not in _TAG_RE.sub("", text)


def _no_partial_entity(text: str) -> bool:
    return re.search(r"&#?[a-zA-Z0-9]*(?![a-zA-Z0-9;])", text) is None


# --- cap_telegram_text ---------------------------------------------------------


def test_text_within_limit_is_returned_unchanged() -> None:
    text = "🔵 from a@example.com:\n<b>Subject</b>\n" + "x" * 100

    assert cap_telegram_text(text) == text
    exact = "a" * TELEGRAM_TEXT_LIMIT
    assert cap_telegram_text(exact) == exact


def test_limit_is_measured_in_utf16_units_not_code_points() -> None:
    # 300 lines of 10 emoji: 3299 code points (< 4096) but 6299 UTF-16 units.
    text = "\n".join(["🟡" * 10] * 300)
    assert len(text) < TELEGRAM_TEXT_LIMIT < _units(text)

    capped = cap_telegram_text(text)

    assert telegram_text_units(capped) == _units(capped)
    assert _units(capped) <= TELEGRAM_TEXT_LIMIT
    assert capped != text
    capped.encode("utf-16-le")  # no lone surrogate


def test_whole_lines_are_kept_and_the_cut_is_marked() -> None:
    lines = [f"<b>line {index:04d}</b> {'z' * 40}" for index in range(200)]
    text = "\n".join(lines)

    capped = cap_telegram_text(text)
    capped_lines = capped.split("\n")

    assert _units(capped) <= TELEGRAM_TEXT_LIMIT
    assert capped_lines[:-1] == lines[: len(capped_lines) - 1]
    assert capped_lines[-1].endswith("…")
    assert _tags_balanced(capped)


def test_oversized_single_line_is_cut_as_plain_text() -> None:
    text = "<b>" + "a" * 5000 + "</b>"

    capped = cap_telegram_text(text)

    assert _units(capped) <= TELEGRAM_TEXT_LIMIT
    assert capped == "a" * (TELEGRAM_TEXT_LIMIT - 1) + "…"


def test_trailing_partial_entity_is_dropped() -> None:
    text = "a" * 17 + "&amp;" + "bbbb"

    capped = cap_telegram_text(text, limit=20)

    assert capped == "a" * 17 + "…"


def test_complete_entity_at_the_cut_is_kept() -> None:
    text = "a" * 14 + "&amp;" + "bbbbbbb"

    capped = cap_telegram_text(text, limit=20)

    assert capped == "a" * 14 + "&amp;" + "…"


def test_tags_left_open_by_dropped_lines_are_closed() -> None:
    text = "<i>first\nsecond\n" + "x" * 50 + "</i>"

    capped = cap_telegram_text(text, limit=30)

    assert capped == "<i>first\nsecond</i>\nxxxxxxxxx…"
    assert _units(capped) <= 30
    assert _tags_balanced(capped)


def test_surrogate_pairs_are_never_split() -> None:
    capped = cap_telegram_text("🟡" * 30, limit=10)

    assert capped == "🟡" * 4 + "…"


def test_long_html_message_is_capped_with_valid_markup() -> None:
    body = "\n".join(
        f"<b>Пункт {index}</b> <i>{_SENTENCE * 2}</i> &amp; ещё" for index in range(60)
    )
    text = f"🔴 from billing@vector.example:\n<b>Счёт</b>\n{body}"

    capped = cap_telegram_text(text)

    assert _units(capped) <= TELEGRAM_TEXT_LIMIT
    assert _tags_balanced(capped)
    assert _no_partial_entity(capped)
    assert capped.startswith("🔴 from billing@vector.example:\n<b>Счёт</b>\n")


# --- runtime paths -----------------------------------------------------------


def _build_context(body: str) -> processor.TelegramBuildContext:
    return processor.TelegramBuildContext(
        email_id=500,
        received_at=datetime(2024, 1, 1, 12, 0),
        priority="🟡",
        from_email="sender@example.com",
        subject="Поставка",
        action_line="Ответить",
        mail_type="",
        body_summary=body,
        body_text=body,
        attachment_summary="",
        attachment_details=[],
        attachment_files=[],
        attachments_count=0,
        extracted_text_len=len(body),
        llm_failed=False,
        signal_invalid=False,
        insights=[],
        insight_digest=None,
        commitments_present=False,
        metadata={"account_email": "account@example.com"},
    )


def test_premium_payload_with_a_6000_char_body_fits_telegram() -> None:
    body = " ".join(
        f"Пункт {i}: поставка товара номер {i * 7} ожидается к дате {i % 28 + 1:02d}.05."
        for i in range(120)
    )
    assert len(body) > 6000

    payload, render_mode, _ = processor.build_telegram_payload(_build_context(body))

    assert render_mode == processor.TelegramRenderMode.FULL
    assert _units(payload.html_text) <= TELEGRAM_TEXT_LIMIT
    assert _tags_balanced(payload.html_text)
    assert _no_partial_entity(payload.html_text)
    assert "sender@example.com" in payload.html_text


def test_minimal_payload_cut_keeps_markup_valid() -> None:
    payload = processor._build_minimal_telegram_payload(
        priority="🔴",
        from_email="sender@example.com",
        subject="Тема " * 300,
        attachments=[],
        metadata={},
        reply_markup=None,
    )

    assert _units(payload.html_text) <= 800
    assert _tags_balanced(payload.html_text)
    assert payload.html_text.endswith("…")


def test_legacy_tg_stage_caps_a_6000_char_text(monkeypatch, tmp_path) -> None:
    from mailbot_v26.config_loader import AccountConfig
    from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor
    from mailbot_v26.tests.integration.harness import build_config

    account = AccountConfig(
        account_id="acc-1",
        login="account@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )
    config = build_config(tmp_path, [account])
    core_pipeline.configure_pipeline(
        config,
        MessageProcessor(SimpleNamespace(), SimpleNamespace()),
        enable_premium_processor=False,
    )
    long_text = "\n".join(f"<b>Пункт {i}</b> {_SENTENCE}" for i in range(120))
    ctx = core_pipeline.PipelineContext(email_id=77, account_email=account.login, uid=1)
    ctx.llm_result = {"text": long_text, "summary": _SENTENCE * 90}
    core_pipeline.PIPELINE_INBOUND_CACHE[77] = InboundMessage(
        subject="", body="Body", sender=""
    )
    sent: list[TelegramPayload] = []

    def _fake_send(payload: TelegramPayload):
        sent.append(payload)
        return telegram_sender.DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(core_pipeline, "send_telegram", _fake_send)
    try:
        core_pipeline.stage_tg(ctx)
    finally:
        core_pipeline.PIPELINE_INBOUND_CACHE.pop(77, None)
        core_pipeline.PIPELINE_CACHE.pop(77, None)

    assert len(sent) == 1
    assert _units(sent[0].html_text) <= TELEGRAM_TEXT_LIMIT
    assert _tags_balanced(sent[0].html_text)


def _oversized_html() -> str:
    return "\n".join(f"<b>{index}</b> <i>{_SENTENCE}</i>" for index in range(120))


def test_sender_safety_net_caps_and_logs(monkeypatch, caplog) -> None:
    posted: list[str] = []

    def _post(**kwargs):
        posted.append(kwargs["text"])
        return SimpleNamespace(
            status_code=200,
            text="ok",
            json=lambda: {"ok": True, "result": {"message_id": 1}},
        )

    monkeypatch.setattr(telegram_sender, "_post_message", _post)
    payload = TelegramPayload(
        html_text=_oversized_html(),
        priority="🔵",
        metadata={"bot_token": "token", "chat_id": "chat"},
    )

    with caplog.at_level(logging.WARNING):
        result = telegram_sender.send_telegram(payload)

    assert result.delivered is True
    assert len(posted) == 1
    assert _units(posted[0]) <= TELEGRAM_TEXT_LIMIT
    assert _tags_balanced(posted[0])
    assert "telegram_text_capped" in caplog.text
    assert _SENTENCE not in caplog.text  # no message content in the log


def test_edit_safety_net_caps_and_logs(monkeypatch, caplog) -> None:
    posted: list[dict] = []

    def _post(url, json=None, timeout=None):  # noqa: A002 - requests signature
        posted.append(dict(json or {}))
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(telegram_sender, "requests", SimpleNamespace(post=_post))

    with caplog.at_level(logging.WARNING):
        applied = telegram_sender.edit_telegram_message(
            bot_token="token",
            chat_id="chat",
            message_id=5,
            html_text=_oversized_html(),
        )

    assert applied is True
    assert _units(posted[0]["text"]) <= TELEGRAM_TEXT_LIMIT
    assert _tags_balanced(posted[0]["text"])
    assert "telegram_text_capped" in caplog.text


@pytest.mark.parametrize("text", ["", "short", "a" * TELEGRAM_TEXT_LIMIT])
def test_sender_does_not_log_a_cap_for_texts_within_limit(
    monkeypatch, caplog, text
) -> None:
    monkeypatch.setattr(
        telegram_sender,
        "_post_message",
        lambda **_kwargs: SimpleNamespace(status_code=200, text="ok", json=lambda: {}),
    )
    payload = TelegramPayload(
        html_text=text, priority="🔵", metadata={"bot_token": "t", "chat_id": "c"}
    )

    with caplog.at_level(logging.WARNING):
        telegram_sender.send_telegram(payload)

    assert "telegram_text_capped" not in caplog.text
