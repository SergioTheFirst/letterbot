"""A non-retryable 400 gets one minimal-payload retry (WP1b).

Telegram refusing the full text (e.g. "message is too long") definitely did
not show it, so one minimal push under the same delivery key keeps
"one email -> one notification". A second refusal settles the email as
DELIVERY_FAILED with one notice. Chat problems ("chat not found"),
401/403/404, markup parse errors (salvaged inside send_telegram), retryable
and maybe-delivered results are never retried this way.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
)
from mailbot_v26.events.contract import EventType
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.system_health import system_health
from mailbot_v26.tests.test_delivery_invariants import (
    FakeTelegram,
    _email_row,
    _patch_premium,
    _queue_rows,
    _Resp,
    _seed_email,
    _setup,
)
from mailbot_v26.worker import telegram_sender
from mailbot_v26.worker.telegram_sender import (
    DeliveryResult,
    should_retry_minimal_payload,
)

_TOO_LONG = '{"ok":false,"error_code":400,"description":"Bad Request: message is too long"}'
_CHAT_NOT_FOUND = '{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}'


def _rejected(
    status_code: int = 400, error: str = _TOO_LONG, *, mode: str = "html"
) -> DeliveryResult:
    return DeliveryResult(
        delivered=False,
        retryable=False,
        error=error,
        mode=mode,
        status_code=status_code,
    )


# --- the retry predicate ------------------------------------------------------


def test_payload_rejection_qualifies_for_a_minimal_retry() -> None:
    assert should_retry_minimal_payload(_rejected()) is True
    assert should_retry_minimal_payload(
        _rejected(error="Bad Request: message text is empty")
    )


@pytest.mark.parametrize(
    "result",
    [
        DeliveryResult(delivered=True, retryable=False, status_code=200),
        _rejected(error=_CHAT_NOT_FOUND),
        _rejected(401, "Unauthorized"),
        _rejected(403, "Forbidden: bot was blocked by the user"),
        _rejected(404, "Not Found"),
        _rejected(error="Bad Request: can't parse entities: unsupported start tag"),
        _rejected(mode="plain_salvage"),
        DeliveryResult(delivered=False, retryable=True, error="429", status_code=429),
        DeliveryResult(delivered=False, retryable=True, error="boom", status_code=500),
        DeliveryResult(
            delivered=False,
            retryable=False,
            error="delivery_unknown:ReadTimeout",
            maybe_delivered=True,
        ),
        DeliveryResult(delivered=False, retryable=False, error="missing required fields"),
    ],
    ids=[
        "delivered",
        "chat_not_found",
        "401",
        "403",
        "404",
        "parse_error",
        "salvage_rejected",
        "429",
        "500",
        "maybe_delivered",
        "no_http_status",
    ],
)
def test_other_failures_are_not_retried_with_a_minimal_payload(result) -> None:
    assert should_retry_minimal_payload(result) is False


# --- premium: _apply_delivery_sla ----------------------------------------------


def _payload(text: str) -> TelegramPayload:
    return TelegramPayload(
        html_text=text, priority="🔵", metadata={"chat_id": "1", "bot_token": "t"}
    )


def _run_sla(results: list[DeliveryResult], *, elapsed: float = 1.0):
    sends: list[str] = []
    edits: list[int] = []

    def _send(payload: TelegramPayload) -> DeliveryResult:
        sends.append(payload.html_text)
        return results.pop(0)

    def _edit(message_id: int, payload: TelegramPayload) -> bool:
        edits.append(message_id)
        return True

    outcome = processor._apply_delivery_sla(
        processing_started_at=0.0,
        wait_budget_seconds=10.0,
        minimal_payload=_payload("minimal"),
        final_payload=_payload("final"),
        send_func=_send,
        edit_func=_edit,
        on_edit_failure=None,
        monotonic=lambda: elapsed,
    )
    return outcome, sends, edits


def test_rejected_final_payload_is_retried_once_as_minimal_without_edit() -> None:
    outcome, sends, edits = _run_sla(
        [_rejected(), DeliveryResult(delivered=True, retryable=False, message_id=9)]
    )

    assert sends == ["final", "minimal"]
    assert edits == []
    assert outcome.delivery_mode == "minimal_after_reject"
    assert outcome.result.delivered is True
    assert outcome.edit_applied is False


def test_rejected_minimal_retry_is_not_retried_again() -> None:
    outcome, sends, _ = _run_sla([_rejected(), _rejected()])

    assert sends == ["final", "minimal"]
    assert outcome.delivery_mode == "minimal_after_reject"
    assert outcome.result.delivered is False
    assert outcome.result.status_code == 400


def test_chat_not_found_is_not_retried() -> None:
    outcome, sends, _ = _run_sla([_rejected(error=_CHAT_NOT_FOUND)])

    assert sends == ["final"]
    assert outcome.delivery_mode == "final_first_send"


def test_rejected_minimal_first_send_is_not_retried() -> None:
    # Past the wait budget the first push is already the minimal payload.
    outcome, sends, edits = _run_sla([_rejected()], elapsed=60.0)

    assert sends == ["minimal"]
    assert edits == []
    assert outcome.delivery_mode == "minimal_then_edit"


# --- end to end through the queue (fake Telegram server) -----------------------


class RejectingTelegram(FakeTelegram):
    """FakeTelegram that also refuses a payload with "message is too long"."""

    def __init__(self) -> None:
        super().__init__()
        self.posted: list[str] = []

    def post_message(self, *, url, chat_id, text, parse_mode, reply_markup):
        self.posted.append(text)
        if self.script and self.script[0] == "400_too_long":
            self.script.pop(0)
            self.calls += 1
            return _Resp(400, text=_TOO_LONG)
        return super().post_message(
            url=url,
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )


@pytest.fixture(autouse=True)
def _isolated_runtime():
    PIPELINE_CACHE.clear()
    PIPELINE_INBOUND_CACHE.clear()
    PIPELINE_RAW_CACHE.clear()
    system_health.reset()
    yield
    PIPELINE_CACHE.clear()
    PIPELINE_INBOUND_CACHE.clear()
    PIPELINE_RAW_CACHE.clear()
    system_health.reset()


@pytest.fixture
def telegram(monkeypatch) -> RejectingTelegram:
    server = RejectingTelegram()
    monkeypatch.setattr(telegram_sender, "_post_message", server.post_message)
    monkeypatch.setattr(
        telegram_sender, "requests", SimpleNamespace(post=server.edit_post)
    )
    return server


@pytest.fixture
def events(monkeypatch) -> SimpleNamespace:
    recorded = SimpleNamespace(contract=[], legacy=[])
    monkeypatch.setattr(
        processor,
        "contract_event_emitter",
        SimpleNamespace(emit=lambda event, *_a, **_k: recorded.contract.append(event)),
    )
    monkeypatch.setattr(
        start_module,
        "event_emitter",
        SimpleNamespace(emit=lambda **kwargs: recorded.legacy.append(kwargs)),
    )
    return recorded


def _delivery_log_rows(storage) -> list[tuple[str, str | None]]:
    return [
        (str(row[0]), row[1])
        for row in storage.conn.execute(
            "SELECT delivery_key, telegram_message_id FROM telegram_delivery_log"
        ).fetchall()
    ]


@pytest.fixture
def minimal_texts(monkeypatch) -> list[str]:
    built: list[str] = []
    original = processor._build_minimal_telegram_payload

    def _spy(**kwargs) -> TelegramPayload:
        payload = original(**kwargs)
        built.append(payload.html_text)
        return payload

    monkeypatch.setattr(processor, "_build_minimal_telegram_payload", _spy)
    return built


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_too_long_rejection_sends_one_minimal_notification(
    tmp_path, monkeypatch, telegram, events, minimal_texts, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    telegram.script = ["400_too_long"]
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.posted) == 2
    assert telegram.posted[1] != telegram.posted[0]
    assert telegram.posted[1] == minimal_texts[-1]
    assert telegram.accepted == [telegram.posted[1]]
    assert telegram.notices() == []
    assert telegram.edits == []
    status, _, delivered_at = _email_row(storage, email_id)
    assert delivered_at
    assert status != "DELIVERY_FAILED"
    assert _queue_rows(storage) == []
    # The retry went out under the email's one delivery key, now confirmed.
    assert _delivery_log_rows(storage) == [(f"email:{email_id}", "1")]
    if premium:
        delivered = [
            event
            for event in events.contract
            if event.event_type == EventType.TELEGRAM_DELIVERED
        ]
        assert [event.payload["delivery_mode"] for event in delivered] == [
            "minimal_after_reject"
        ]


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_rejected_minimal_retry_marks_failed_with_one_notice(
    tmp_path, monkeypatch, telegram, events, minimal_texts, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    telegram.script = ["400_too_long", "400_too_long"]
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.email_messages() == []
    assert len(telegram.notices()) == 1
    # Full payload, its minimal retry, then the failure notice.
    assert len(telegram.posted) == 3
    assert telegram.posted[1] == minimal_texts[-1]
    assert _email_row(storage, email_id)[0] == "DELIVERY_FAILED"
    assert _queue_rows(storage) == []
    assert _delivery_log_rows(storage) == []  # definitely not shown: key released


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_chat_not_found_is_not_retried_end_to_end(
    tmp_path, monkeypatch, telegram, events, minimal_texts, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    telegram.script = ["400"]  # FakeTelegram: "chat not found"
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.email_messages() == []
    assert len(telegram.notices()) == 1
    assert len(telegram.posted) == 2  # the email once, then the notice
    assert _email_row(storage, email_id)[0] == "DELIVERY_FAILED"
