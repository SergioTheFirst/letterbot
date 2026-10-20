"""WP1c: honour Telegram's 429 retry_after.

A 429 reply names how long to wait (``parameters.retry_after``, else the
Retry-After header). The sender never sleeps; it reports the value, and the
queue waits at least that long before the next attempt, in the legacy TG
backoff and in the premium -> legacy fallback alike.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.tests.test_delivery_invariants import (  # noqa: F401 - fixtures
    FakeTelegram,
    _Resp,
    _email_row,
    _isolated_runtime,
    _make_ready,
    _patch_premium,
    _queue_rows,
    _seed_email,
    _setup,
)
from mailbot_v26.tests import test_delivery_invariants as _invariants

# Shared fixture, re-exported by assignment so pytest discovers it here.
events = _invariants.events
from mailbot_v26.worker import telegram_sender


class _RateLimitedTelegram(FakeTelegram):
    """FakeTelegram that also answers "429" with Telegram's retry_after body."""

    def __init__(self) -> None:
        super().__init__()
        self.retry_after = 30

    def post_message(self, *, url, chat_id, text, parse_mode, reply_markup):
        if self.script and self.script[0] == "429":
            self.script.pop(0)
            self.calls += 1
            return _Resp(
                429,
                {
                    "ok": False,
                    "error_code": 429,
                    "description": f"Too Many Requests: retry after {self.retry_after}",
                    "parameters": {"retry_after": self.retry_after},
                },
            )
        return super().post_message(
            url=url,
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )


@pytest.fixture
def tg429(monkeypatch) -> _RateLimitedTelegram:
    server = _RateLimitedTelegram()
    monkeypatch.setattr(telegram_sender, "_post_message", server.post_message)
    monkeypatch.setattr(
        telegram_sender, "requests", SimpleNamespace(post=server.edit_post)
    )
    return server


def _payload() -> TelegramPayload:
    return TelegramPayload(
        html_text="Hello",
        priority="🔵",
        metadata={"bot_token": "token", "chat_id": "chat"},
    )


class _HeaderResp(_Resp):
    def __init__(self, status_code: int, payload: dict | None, headers: dict):
        super().__init__(status_code, payload, text="Too Many Requests")
        self.headers = headers


# --- sender: parse retry_after -------------------------------------------------


def test_429_result_carries_telegram_retry_after(monkeypatch) -> None:
    monkeypatch.setattr(
        telegram_sender,
        "_post_message",
        lambda **_kwargs: _Resp(
            429,
            {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests: retry after 30",
                "parameters": {"retry_after": 30},
            },
        ),
    )

    result = telegram_sender.send_telegram(_payload())

    assert result.delivered is False
    assert result.retryable is True
    assert result.maybe_delivered is False
    assert result.status_code == 429
    assert result.retry_after == 30.0


@pytest.mark.parametrize(
    ("body", "headers", "expected"),
    [
        (None, {"Retry-After": "12"}, 12.0),
        ({"ok": False, "parameters": {"retry_after": "soon"}}, {"Retry-After": "5"}, 5.0),
        ({"ok": False, "parameters": {"retry_after": 0}}, {}, 1.0),
        ({"ok": False, "parameters": {"retry_after": 99999}}, {}, 3600.0),
        ({"ok": False}, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}, None),
        ({"ok": False}, {}, None),
    ],
    ids=["header", "bad-json-value-header", "clamp-low", "clamp-high", "http-date", "none"],
)
def test_429_retry_after_falls_back_to_header_and_is_clamped(
    monkeypatch, body, headers, expected
) -> None:
    monkeypatch.setattr(
        telegram_sender,
        "_post_message",
        lambda **_kwargs: _HeaderResp(429, body, headers),
    )

    result = telegram_sender.send_telegram(_payload())

    assert result.retryable is True
    assert result.retry_after == expected


def test_retry_after_is_only_read_for_429(monkeypatch) -> None:
    monkeypatch.setattr(
        telegram_sender,
        "_post_message",
        lambda **_kwargs: _HeaderResp(503, None, {"Retry-After": "30"}),
    )

    result = telegram_sender.send_telegram(_payload())

    assert result.retryable is True
    assert result.retry_after is None


# --- queue: wait at least retry_after -------------------------------------------


def test_queue_backoff_is_stretched_to_retry_after_only_for_telegram_errors() -> None:
    error = start_module.TelegramRetryableError("429", retry_after=30.2)
    assert start_module._queue_backoff_seconds(1, error) == 31
    assert start_module._queue_backoff_seconds(5, error) == 320
    assert start_module._queue_backoff_seconds(9, error) == 600
    huge = start_module.TelegramRetryableError("429", retry_after=3600)
    assert start_module._queue_backoff_seconds(1, huge) == 3600
    other = RuntimeError("boom")
    other.retry_after = 3600  # type: ignore[attr-defined]
    assert start_module._queue_backoff_seconds(1, other) == 20
    assert start_module._queue_backoff_seconds(
        1, start_module.TelegramRetryableError("5xx")
    ) == 20


def test_legacy_429_waits_retry_after_before_the_next_attempt(
    tmp_path, monkeypatch, tg429, events, caplog
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    tg429.script = ["429"]
    email_id = _seed_email(storage, stage="TG")
    before = datetime.utcnow()

    with caplog.at_level("INFO"):
        start_module._process_queue(storage, config, legacy_processor, flags)

    assert tg429.accepted == []
    stage, not_before = storage.conn.execute(
        "SELECT stage, not_before FROM queue WHERE email_id = ?", (email_id,)
    ).fetchone()
    assert stage == "TG"
    # First attempt backoff is 20 s; Telegram asked for 30 s.
    assert datetime.fromisoformat(not_before) >= before + timedelta(seconds=30)
    assert "error_class=TelegramRetryableError" in caplog.text
    retries = [e for e in events.legacy if e.get("type") == "telegram_delivery_retry"]
    assert [e["payload"]["backoff_seconds"] for e in retries] == [30]
    assert retries[0]["payload"]["error_class"] == "TelegramRetryableError"

    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(tg429.accepted) == 1
    assert _queue_rows(storage) == []
    assert _email_row(storage, email_id)[2]


def test_premium_429_falls_back_to_legacy_not_before_retry_after(
    tmp_path, monkeypatch, tg429, events
) -> None:
    _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=True
    )
    tg429.retry_after = 120
    tg429.script = ["429"]
    email_id = _seed_email(storage, stage="PARSE")
    before = datetime.utcnow()

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert tg429.calls == 1  # the legacy chain waits instead of posting at once
    rows = storage.conn.execute(
        "SELECT stage, not_before FROM queue WHERE email_id = ?", (email_id,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "LLM"
    assert rows[0][1] is not None
    not_before = datetime.fromisoformat(rows[0][1])
    assert not_before.tzinfo is None  # naive UTC, compared as text (F11)
    assert not_before >= before + timedelta(seconds=120)

    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(tg429.accepted) == 1
    assert _queue_rows(storage) == []
