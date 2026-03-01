from __future__ import annotations

import logging

import pytest

from mailbot_v26.pipeline.stage_telegram import enqueue_tg
from mailbot_v26.pipeline.telegram_payload import TelegramPayload


def test_enqueue_tg_missing_credentials_keeps_existing_behavior(caplog: pytest.LogCaptureFixture) -> None:
    payload = TelegramPayload(
        html_text="text",
        priority="🔵",
        metadata={"chat_id": "123", "bot_token": ""},
    )

    with caplog.at_level(logging.ERROR):
        result = enqueue_tg(email_id=1, payload=payload)

    assert result.delivered is False
    assert result.error == "missing telegram credentials"
    assert "telegram_delivery_failed" in caplog.text
