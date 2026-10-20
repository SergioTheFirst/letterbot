"""P2SEC: the error a failed send hands back is stored (emails, queue, events,
delivery log), so worker.telegram_sender redacts it and caps it at 300
characters at the source; redaction runs first, so a cut never leaves part of
a bot token behind."""

from __future__ import annotations

from types import SimpleNamespace

import requests
import urllib3

from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.worker import telegram_sender

TOKEN = "987654321:AAHfakeTokenValueForTestsOnly_1234567"
TOKEN_SECRET = TOKEN.split(":", 1)[1]
MAX_STORED_ERROR_CHARS = 300


def _payload() -> TelegramPayload:
    return TelegramPayload(
        html_text="hello",
        priority="\U0001f535",
        metadata={"bot_token": TOKEN, "chat_id": "42"},
    )


def _response(status_code: int, text: str) -> SimpleNamespace:
    return SimpleNamespace(status_code=status_code, text=text)


def _reply_with(monkeypatch, *responses: SimpleNamespace) -> None:
    pending = list(responses)
    monkeypatch.setattr(telegram_sender, "_post_message", lambda **_kw: pending.pop(0))


def test_cap_is_300_characters() -> None:
    assert telegram_sender.MAX_STORED_ERROR_CHARS == MAX_STORED_ERROR_CHARS


def test_long_http_error_body_is_capped(monkeypatch) -> None:
    proxy_page = "<html>" + "x" * 5000 + "</html>"  # e.g. a proxy's 502 page
    _reply_with(monkeypatch, _response(502, proxy_page))

    result = telegram_sender.send_telegram(_payload())

    assert result.retryable is True
    assert len(result.error or "") == MAX_STORED_ERROR_CHARS
    assert (result.error or "").startswith("<html>xxx")
    assert (result.error or "").endswith("…")


def test_error_body_at_the_cap_is_kept_and_one_more_char_is_cut(monkeypatch) -> None:
    _reply_with(
        monkeypatch,
        _response(500, "a" * MAX_STORED_ERROR_CHARS),
        _response(500, "b" * (MAX_STORED_ERROR_CHARS + 1)),
    )

    at_cap = telegram_sender.send_telegram(_payload())
    over_cap = telegram_sender.send_telegram(_payload())

    assert at_cap.error == "a" * MAX_STORED_ERROR_CHARS
    assert over_cap.error == "b" * (MAX_STORED_ERROR_CHARS - 1) + "…"


def test_short_telegram_error_is_kept_verbatim(monkeypatch) -> None:
    body = '{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}'
    _reply_with(monkeypatch, _response(400, body))

    result = telegram_sender.send_telegram(_payload())

    assert result.error == body
    assert result.status_code == 400


def test_http_error_body_echoing_the_token_is_redacted(monkeypatch) -> None:
    _reply_with(monkeypatch, _response(404, f"Not Found: /bot{TOKEN}/sendMessage"))

    result = telegram_sender.send_telegram(_payload())

    assert TOKEN_SECRET not in (result.error or "")
    assert "bot<redacted>" in (result.error or "")


def test_long_http_error_body_is_redacted_before_it_is_cut(monkeypatch) -> None:
    # The body is the only source here (no earlier redaction). The bare token
    # straddles the cut: cutting first would leave a secret fragment too short
    # for the redaction pattern.
    _reply_with(monkeypatch, _response(502, "x" * 280 + f" {TOKEN} tail"))

    result = telegram_sender.send_telegram(_payload())

    assert len(result.error or "") == MAX_STORED_ERROR_CHARS
    assert TOKEN_SECRET[:4] not in (result.error or "")
    assert "<redacted-token>" in (result.error or "")


def test_long_salvage_error_body_is_capped(monkeypatch) -> None:
    _reply_with(
        monkeypatch,
        _response(400, "Bad Request: can't parse entities"),
        _response(500, "e" * 1000),
    )

    result = telegram_sender.send_telegram(_payload())

    assert result.mode == "plain_salvage"
    assert len(result.error or "") == MAX_STORED_ERROR_CHARS


def test_long_send_exception_text_is_redacted_before_it_is_cut(monkeypatch) -> None:
    # Not sent (connection refused): the exception text is stored. The bare
    # token straddles the cut, where cutting first would leave part of its
    # secret too short for the redaction pattern.
    exc = requests.ConnectionError(
        urllib3.exceptions.MaxRetryError(
            "x" * 250,
            f"/{TOKEN}",
            reason=urllib3.exceptions.NewConnectionError(None, "boom"),
        )
    )

    def _raise(**_kwargs):
        raise exc

    monkeypatch.setattr(telegram_sender, "_post_message", _raise)

    result = telegram_sender.send_telegram(_payload())

    assert result.retryable is True
    assert len(result.error or "") == MAX_STORED_ERROR_CHARS
    assert TOKEN_SECRET[:4] not in (result.error or "")
