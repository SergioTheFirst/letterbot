"""Delivery invariants (WP1a): one email -> at most one Telegram message.

The fake Telegram server records every sendMessage it accepts. A ReadTimeout
after acceptance models the at-most-once trade-off: the message reached the
chat, the client cannot know it, so the email becomes DELIVERY_UNKNOWN and is
never sent again (and never gets a "delivery failed" notice).
"""

from __future__ import annotations

import errno
import json
import ssl
import zipfile
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import urllib3

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
    PipelineContext,
    configure_pipeline,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor
from mailbot_v26.pipeline.telegram_payload import TelegramPayload
from mailbot_v26.system_health import system_health
from mailbot_v26.tests.integration.harness import build_config
from mailbot_v26.tests.integration.test_degradation_scenarios import (
    _llm_result,
    _patch_processor_basics,
)
from mailbot_v26.worker import telegram_sender

_ACCOUNT = AccountConfig(
    account_id="acc-1",
    login="account@example.com",
    password="pw",
    host="imap.example.com",
    port=993,
    use_ssl=True,
    telegram_chat_id="chat",
)
_FAILURE_NOTICE_MARKERS = (
    "Доставка в Telegram не удалась",
    "Telegram delivery failed",
    "СБОЙ ДОСТАВКИ В TELEGRAM",
    "TELEGRAM DELIVERY FAILED",
)


class _Resp:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload


class FakeTelegram:
    """sendMessage stand-in driven by a script of actions (default "ok")."""

    def __init__(self) -> None:
        self.script: list[str] = []
        self.accepted: list[str] = []
        self.calls = 0
        self.edits: list[dict] = []

    def post_message(self, *, url, chat_id, text, parse_mode, reply_markup):
        self.calls += 1
        action = self.script.pop(0) if self.script else "ok"
        if action == "connect_timeout":
            raise requests.exceptions.ConnectTimeout("connect timed out")
        if action == "500":
            return _Resp(500, text="Internal Server Error")
        if action == "400":
            return _Resp(400, text='{"ok":false,"description":"Bad Request: chat not found"}')
        self.accepted.append(text)
        if action == "read_timeout":
            raise requests.exceptions.ReadTimeout("read timed out")
        return _Resp(200, {"ok": True, "result": {"message_id": len(self.accepted)}})

    def edit_post(self, url, json=None, timeout=None):  # noqa: A002 - requests signature
        self.edits.append(dict(json or {}))
        return _Resp(200, {"ok": True, "result": True})

    def email_messages(self) -> list[str]:
        return [text for text in self.accepted if not _is_failure_notice(text)]

    def notices(self) -> list[str]:
        return [text for text in self.accepted if _is_failure_notice(text)]


def _is_failure_notice(text: str) -> bool:
    return any(marker in text for marker in _FAILURE_NOTICE_MARKERS)


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
def telegram(monkeypatch) -> FakeTelegram:
    server = FakeTelegram()
    monkeypatch.setattr(telegram_sender, "_post_message", server.post_message)
    # Edits call requests.post directly; nothing may reach the network.
    monkeypatch.setattr(
        telegram_sender, "requests", SimpleNamespace(post=server.edit_post)
    )
    return server


@pytest.fixture
def events(monkeypatch) -> SimpleNamespace:
    recorded = SimpleNamespace(contract=[], legacy=[])
    recorder = SimpleNamespace(emit=lambda event, *_a, **_k: recorded.contract.append(event))
    monkeypatch.setattr(processor, "contract_event_emitter", recorder)
    monkeypatch.setattr(
        start_module,
        "event_emitter",
        SimpleNamespace(emit=lambda **kwargs: recorded.legacy.append(kwargs)),
    )
    return recorded


def _patch_premium(monkeypatch, events: SimpleNamespace, *, llm_result=None) -> None:
    _patch_processor_basics(
        monkeypatch, llm_result=llm_result or _llm_result(provider="cloudflare")
    )
    # _patch_processor_basics silences the contract emitter; record it again.
    monkeypatch.setattr(
        processor,
        "contract_event_emitter",
        SimpleNamespace(emit=lambda event, *_a, **_k: events.contract.append(event)),
    )
    monkeypatch.setattr(
        processor,
        "notification_alert_store",
        SimpleNamespace(
            record_failure=lambda *_a, **_k: 1,
            reset_failures=lambda *_a, **_k: None,
            should_alert=lambda **_k: False,
            save_alert=lambda *_a, **_k: None,
        ),
    )


def _fake_legacy_llm(ctx: PipelineContext) -> None:
    ctx.llm_result = {"text": "Legacy telegram text"}


def _raw_email() -> bytes:
    message = EmailMessage()
    message["Subject"] = "Subject"
    message["From"] = "Sender <sender@example.com>"
    message["To"] = "account@example.com"
    message["Date"] = "Tue, 02 Jan 2024 10:00:00 +0000"
    message["Message-ID"] = "<m1@example.com>"
    message.set_content("Body")
    return message.as_bytes()


def _setup(tmp_path: Path, monkeypatch, *, premium: bool):
    config = build_config(tmp_path, [_ACCOUNT])
    storage = Storage(config.storage.db_path)
    legacy_processor = MessageProcessor(SimpleNamespace(), SimpleNamespace())
    configure_pipeline(config, legacy_processor, enable_premium_processor=premium)
    flags = FeatureFlags(base_dir=tmp_path)
    flags.ENABLE_PREMIUM_PROCESSOR = premium
    monkeypatch.setattr(start_module, "stage_llm", _fake_legacy_llm)
    return config, storage, legacy_processor, flags


def _seed_context(email_id: int, *, stage: str, uid: int = 1) -> None:
    ctx = PipelineContext(email_id=email_id, account_email=_ACCOUNT.login, uid=uid)
    if stage == "TG":
        ctx.llm_result = {"text": "Telegram message"}
    PIPELINE_CACHE[email_id] = ctx
    PIPELINE_INBOUND_CACHE[email_id] = InboundMessage(
        subject="Subject", body="Body", sender="sender@example.com"
    )
    PIPELINE_RAW_CACHE[email_id] = _raw_email()


def _seed_email(storage: Storage, *, stage: str, uid: int = 1) -> int:
    email_id = storage.upsert_email(
        account_email=_ACCOUNT.login,
        uid=uid,
        message_id=f"<m{uid}@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Subject",
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
    )
    storage.enqueue_stage(email_id, stage)
    _seed_context(email_id, stage=stage, uid=uid)
    return email_id


def _make_ready(storage: Storage) -> None:
    with storage.conn:
        storage.conn.execute("UPDATE queue SET not_before = NULL")


def _set_attempts(storage: Storage, email_id: int, attempts: int) -> None:
    with storage.conn:
        storage.conn.execute(
            "UPDATE queue SET attempts = ? WHERE email_id = ?", (attempts, email_id)
        )


def _email_row(storage: Storage, email_id: int) -> tuple[str, str | None, str | None]:
    row = storage.conn.execute(
        "SELECT status, error_last, telegram_delivered_at FROM emails WHERE id = ?",
        (email_id,),
    ).fetchone()
    return str(row[0]), row[1], row[2]


def _queue_rows(storage: Storage) -> list[tuple[str, int, str | None]]:
    return [
        (str(row[0]), int(row[1]), row[2])
        for row in storage.conn.execute(
            "SELECT stage, attempts, last_error FROM queue ORDER BY id"
        ).fetchall()
    ]


def _unknown_contract_events(events: SimpleNamespace) -> list[EventV1]:
    return [
        event
        for event in events.contract
        if event.event_type == EventType.TELEGRAM_FAILED
        and event.payload.get("delivery_state") == "unknown"
    ]


# --- accepted-then-timeout: exactly one message, DELIVERY_UNKNOWN -------------


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_read_timeout_after_accept_sends_once_across_retry_fallback_and_restart(
    tmp_path, monkeypatch, telegram, events, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    telegram.script = ["read_timeout"]
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")

    start_module._process_queue(storage, config, legacy_processor, flags)
    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    # Restart: new Storage on the same file, a stale TG item and a rebuilt context.
    restarted = Storage(config.storage.db_path)
    restarted.enqueue_stage(email_id, "TG")
    _seed_context(email_id, stage="TG")
    start_module._process_queue(restarted, config, legacy_processor, flags)

    assert len(telegram.accepted) == 1
    assert telegram.notices() == []
    status, error_last, _ = _email_row(restarted, email_id)
    assert status == "DELIVERY_UNKNOWN"
    assert error_last and "delivery_unknown" in error_last
    assert _queue_rows(restarted) == []
    unknown_events = _unknown_contract_events(events)
    assert unknown_events
    assert unknown_events[0].payload["error"] == "delivery_unknown"
    assert unknown_events[0].payload["delivered"] is False
    assert unknown_events[0].payload["error_class"] == "ReadTimeout"
    if not premium:  # the premium processor emits its legacy event itself
        assert any(
            item.get("type") == "telegram_delivery_failed"
            and item.get("payload", {}).get("delivery_state") == "unknown"
            for item in events.legacy
        )


def test_legacy_unknown_degrades_telegram_until_the_next_delivery(
    tmp_path, monkeypatch, telegram, events
) -> None:
    from mailbot_v26.system_health import OperationalMode

    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    telegram.script = ["read_timeout"]
    _seed_email(storage, stage="TG", uid=1)
    start_module._process_queue(storage, config, legacy_processor, flags)
    assert system_health.mode == OperationalMode.DEGRADED_NO_TELEGRAM

    second = _seed_email(storage, stage="TG", uid=2)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert system_health.mode == OperationalMode.FULL
    assert len(telegram.accepted) == 2
    assert _email_row(storage, second)[2]


# --- definitely-not-sent errors are retried and delivered once ----------------


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_connect_timeout_is_retried_and_delivered_exactly_once(
    tmp_path, monkeypatch, telegram, events, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    telegram.script = ["connect_timeout"]
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")

    start_module._process_queue(storage, config, legacy_processor, flags)
    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.accepted) == 1
    assert telegram.notices() == []
    status, _, delivered_at = _email_row(storage, email_id)
    assert status not in {"DELIVERY_FAILED", "DELIVERY_UNKNOWN"}
    assert delivered_at
    assert _queue_rows(storage) == []


# --- bookkeeping failure after success never causes a resend -----------------


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_mark_done_failure_after_success_does_not_resend_on_next_run(
    tmp_path, monkeypatch, telegram, events, premium
) -> None:
    import sqlite3

    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")
    real_mark_done = storage.mark_done
    failures = {"left": 1}

    def flaky_mark_done(queue_id: int) -> None:
        if failures["left"]:
            failures["left"] -= 1
            raise sqlite3.OperationalError("database is locked")
        real_mark_done(queue_id)

    monkeypatch.setattr(storage, "mark_done", flaky_mark_done)

    start_module._process_queue(storage, config, legacy_processor, flags)
    assert len(telegram.accepted) == 1
    _make_ready(storage)
    _seed_context(email_id, stage="PARSE" if premium else "TG")
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.accepted) == 1
    assert telegram.notices() == []
    assert _queue_rows(storage) == []
    status, _, delivered_at = _email_row(storage, email_id)
    assert delivered_at
    assert status not in {"DELIVERY_FAILED", "DELIVERY_UNKNOWN", "ERROR"}


# --- reservation store unavailable: fail closed ------------------------------


@pytest.mark.parametrize("premium", [True, False], ids=["premium", "legacy"])
def test_unavailable_reservation_store_sends_nothing_then_retries(
    tmp_path, monkeypatch, telegram, events, premium
) -> None:
    if premium:
        _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=premium
    )
    email_id = _seed_email(storage, stage="PARSE" if premium else "TG")
    real_reserve = storage.reserve_telegram_delivery
    monkeypatch.setattr(
        storage, "reserve_telegram_delivery", lambda **_kwargs: "unavailable"
    )

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.calls == 0
    rows = _queue_rows(storage)
    assert len(rows) == 1
    assert rows[0][0] == "TG"
    assert "delivery_log_unavailable" in (rows[0][2] or "")

    monkeypatch.setattr(storage, "reserve_telegram_delivery", real_reserve)
    _make_ready(storage)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert len(telegram.accepted) == 1
    assert _queue_rows(storage) == []
    assert _email_row(storage, email_id)[2]


def test_unavailable_reservation_store_at_last_attempt_marks_failed_with_notice(
    tmp_path, monkeypatch, telegram, events
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    _set_attempts(storage, email_id, start_module.MAX_TELEGRAM_STAGE_ATTEMPTS - 1)
    monkeypatch.setattr(
        storage, "reserve_telegram_delivery", lambda **_kwargs: "unavailable"
    )

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.email_messages() == []
    assert len(telegram.notices()) == 1
    assert "delivery_log_unavailable" in telegram.notices()[0]
    status, error_last, _ = _email_row(storage, email_id)
    assert status == "DELIVERY_FAILED"
    assert error_last == "delivery_log_unavailable"
    assert _queue_rows(storage) == []


# --- premium no-send (F1) and non-retryable failure ---------------------------


def test_premium_llm_empty_result_falls_back_to_legacy_and_notifies_once(
    tmp_path, monkeypatch, telegram, events
) -> None:
    _patch_premium(monkeypatch, events)
    # Both the LLM and its heuristic stand-in come back empty: the premium
    # processor takes its llm_empty branch and returns without sending.
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: None)
    monkeypatch.setattr(
        processor, "_build_heuristic_llm_result", lambda **_kwargs: None
    )
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=True
    )
    email_id = _seed_email(storage, stage="PARSE")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.calls == 1  # the legacy TG stage, not premium
    assert len(telegram.accepted) == 1
    assert _queue_rows(storage) == []
    assert _email_row(storage, email_id)[2]


def test_premium_non_retryable_rejection_marks_failed_and_sends_one_notice(
    tmp_path, monkeypatch, telegram, events
) -> None:
    _patch_premium(monkeypatch, events)
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=True
    )
    telegram.script = ["400"]
    email_id = _seed_email(storage, stage="PARSE")

    start_module._process_queue(storage, config, legacy_processor, flags)

    assert telegram.email_messages() == []
    assert len(telegram.notices()) == 1
    assert _email_row(storage, email_id)[0] == "DELIVERY_FAILED"
    assert _queue_rows(storage) == []


# --- fail-open sends at most once --------------------------------------------


def _fail_open_processor() -> SimpleNamespace:
    return SimpleNamespace(process=lambda _login, _inbound: "Fail-open text")


def test_fail_open_after_delivered_email_sends_nothing(
    tmp_path, monkeypatch, telegram, events
) -> None:
    config, storage, _legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    start_module._process_queue(storage, config, _fail_open_processor(), flags)
    assert len(telegram.accepted) == 1

    # A later PARSE item for the same email dead-letters and triggers fail-open.
    storage.enqueue_stage(email_id, "PARSE")
    _set_attempts(storage, email_id, start_module.MAX_PIPELINE_STAGE_ATTEMPTS - 1)
    _seed_context(email_id, stage="PARSE")
    monkeypatch.setattr(
        start_module,
        "stage_parse",
        lambda _ctx: (_ for _ in ()).throw(RuntimeError("parse boom")),
    )
    start_module._process_queue(storage, config, _fail_open_processor(), flags)

    assert len(telegram.accepted) == 1
    assert "Fail-open text" not in telegram.accepted[0]


def test_fail_open_twice_for_same_email_sends_once(
    tmp_path, monkeypatch, telegram, events
) -> None:
    config, storage, _legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    monkeypatch.setattr(
        start_module,
        "stage_parse",
        lambda _ctx: (_ for _ in ()).throw(RuntimeError("parse boom")),
    )
    email_id = _seed_email(storage, stage="PARSE")
    for _ in range(2):
        storage.enqueue_stage(email_id, "PARSE")
        _set_attempts(storage, email_id, start_module.MAX_PIPELINE_STAGE_ATTEMPTS - 1)
        _seed_context(email_id, stage="PARSE")
        start_module._process_queue(storage, config, _fail_open_processor(), flags)

    assert telegram.accepted == ["Fail-open text"]
    assert _email_row(storage, email_id)[2]


# --- EmailDeliveryGuard + storage seams ---------------------------------------


def _guard(storage: Storage, email_id: int):
    from mailbot_v26.pipeline.delivery_guard import EmailDeliveryGuard

    return EmailDeliveryGuard(
        storage, email_id=email_id, account_id=_ACCOUNT.login, chat_id="chat"
    )


def _plain_email(storage: Storage, uid: int = 1) -> int:
    return storage.upsert_email(
        account_email=_ACCOUNT.login,
        uid=uid,
        message_id=None,
        from_email=None,
        from_name=None,
        subject=None,
        received_at=None,
        attachments_count=0,
    )


def test_guard_reserves_once_and_confirms_delivery(tmp_path) -> None:
    from mailbot_v26.worker.telegram_sender import DeliveryResult

    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    guard = _guard(storage, email_id)

    assert guard.outcome == "none"
    assert guard.before_send() is None
    assert guard.before_send() is None  # idempotent while the key is held
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) == "unconfirmed"
    guard.after_send(DeliveryResult(delivered=True, retryable=False, message_id=None))

    assert guard.outcome == "delivered"
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) == "confirmed"
    assert storage.is_telegram_delivered(email_id)
    blocked = guard.before_send()
    assert blocked is not None and blocked.delivered is False

    second = _guard(storage, email_id)
    blocked = second.before_send()
    assert blocked is not None
    assert blocked.maybe_delivered is True
    assert blocked.error == "duplicate_suppressed"
    assert second.outcome == "duplicate"


def test_guard_turns_unconfirmed_reservation_into_unknown(tmp_path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    assert _guard(storage, email_id).before_send() is None  # crash before POST

    guard = _guard(storage, email_id)
    blocked = guard.before_send()

    assert blocked is not None and blocked.maybe_delivered is True
    assert guard.outcome == "unknown"


def test_guard_releases_key_for_definitely_not_sent_results(tmp_path) -> None:
    from mailbot_v26.worker.telegram_sender import DeliveryResult

    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    guard = _guard(storage, email_id)
    assert guard.before_send() is None
    guard.after_send(
        DeliveryResult(
            delivered=False, retryable=True, error="429", status_code=429, retry_after=30.0
        )
    )
    assert guard.outcome == "retryable"
    assert guard.retry_after == 30.0
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) is None

    failed = _guard(storage, email_id)
    assert failed.before_send() is None
    failed.after_send(DeliveryResult(delivered=False, retryable=False, error="400"))
    assert failed.outcome == "failed"
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) is None

    aborted = _guard(storage, email_id)
    assert aborted.before_send() is None
    aborted.abort()
    assert aborted.outcome == "none"
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) is None


def test_guard_keeps_tombstone_for_maybe_delivered(tmp_path) -> None:
    from mailbot_v26.worker.telegram_sender import DeliveryResult

    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    guard = _guard(storage, email_id)
    assert guard.before_send() is None
    guard.after_send(
        DeliveryResult(
            delivered=False,
            retryable=False,
            error="delivery_unknown:ReadTimeout",
            maybe_delivered=True,
        )
    )
    assert guard.outcome == "unknown"
    assert storage.get_telegram_delivery_state(f"email:{email_id}", email_id) == "unconfirmed"


def test_guard_fails_closed_when_reservation_store_unavailable(tmp_path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    storage.reserve_telegram_delivery = lambda **_kwargs: "unavailable"  # type: ignore[method-assign]
    guard = _guard(storage, email_id)

    blocked = guard.before_send()

    assert blocked is not None
    assert blocked.delivered is False and blocked.retryable is True
    assert blocked.error == "delivery_log_unavailable"
    assert guard.outcome == "retryable"


def test_storage_delivery_unknown_status_and_state(tmp_path) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    email_id = _plain_email(storage)
    key = f"email:{email_id}"
    assert storage.get_telegram_delivery_state(key, email_id) is None

    storage.set_email_delivery_unknown(email_id, "delivery_unknown:ReadTimeout")
    status, error_last, _ = _email_row(storage, email_id)
    assert status == "DELIVERY_UNKNOWN"
    assert error_last == "delivery_unknown:ReadTimeout"

    # A delivery recorded before the reservation log existed counts as confirmed.
    other = _plain_email(storage, uid=2)
    storage.mark_telegram_delivered(other)
    assert storage.get_telegram_delivery_state(f"email:{other}", other) == "confirmed"


# --- exception classification -------------------------------------------------


def _payload() -> TelegramPayload:
    return TelegramPayload(
        html_text="hello",
        priority="🔵",
        metadata={"bot_token": "123456:SECRET", "chat_id": "chat"},
    )


def _cert_error() -> Exception:
    reason = urllib3.exceptions.SSLError(
        ssl.SSLCertVerificationError(1, "certificate verify failed")
    )
    return requests.exceptions.SSLError(
        urllib3.exceptions.MaxRetryError(None, "/bot/sendMessage", reason=reason)
    )


def _refused_error() -> Exception:
    reason = urllib3.exceptions.NewConnectionError(None, "Connection refused")
    return requests.exceptions.ConnectionError(
        urllib3.exceptions.MaxRetryError(None, "/bot/sendMessage", reason=reason)
    )


def _reset_error() -> Exception:
    return requests.exceptions.ConnectionError(
        urllib3.exceptions.ProtocolError(
            "Connection aborted.", ConnectionResetError(errno.ECONNRESET, "reset")
        )
    )


@pytest.mark.parametrize(
    ("exc", "retryable", "maybe_delivered"),
    [
        (requests.exceptions.ReadTimeout("read timed out"), False, True),
        (requests.exceptions.ConnectTimeout("connect timed out"), True, False),
        (requests.exceptions.ProxyError("proxy down"), True, False),
        (_refused_error(), True, False),
        (_reset_error(), False, True),
        (_cert_error(), True, False),
        (requests.exceptions.SSLError("EOF occurred in violation of protocol"), False, True),
        (requests.exceptions.ChunkedEncodingError("broken chunk"), False, True),
        (requests.exceptions.ContentDecodingError("bad gzip"), False, True),
        (requests.exceptions.InvalidURL("bad url"), False, False),
        (requests.exceptions.MissingSchema("no schema"), False, False),
        (requests.exceptions.InvalidHeader("bad header"), False, False),
        (ValueError("unexpected"), False, True),
    ],
    ids=lambda value: type(value).__name__ if isinstance(value, Exception) else None,
)
def test_send_exception_classification(monkeypatch, exc, retryable, maybe_delivered) -> None:
    def _raise(**_kwargs):
        raise exc

    monkeypatch.setattr(telegram_sender, "_post_message", _raise)

    result = telegram_sender.send_telegram(_payload())

    assert result.delivered is False
    assert result.retryable is retryable
    assert result.maybe_delivered is maybe_delivered
    if maybe_delivered:
        assert result.error == f"delivery_unknown:{type(exc).__name__}"
    assert "SECRET" not in (result.error or "")


def test_http_results_carry_status_code(monkeypatch) -> None:
    responses = [_Resp(500, text="boom"), _Resp(200, {"ok": True, "result": {"message_id": 7}})]
    monkeypatch.setattr(
        telegram_sender, "_post_message", lambda **_kwargs: responses.pop(0)
    )

    failed = telegram_sender.send_telegram(_payload())
    sent = telegram_sender.send_telegram(_payload())

    assert (failed.status_code, failed.retryable, failed.maybe_delivered) == (500, True, False)
    assert (sent.status_code, sent.delivered, sent.message_id) == (200, True, 7)


# --- /status and SLA ------------------------------------------------------------


def test_sla_counts_unconfirmed_deliveries_and_status_shows_them(tmp_path) -> None:
    from mailbot_v26.observability.notification_sla import compute_notification_sla
    from mailbot_v26.storage.analytics import KnowledgeAnalytics
    from mailbot_v26.tests.test_telegram_inbound import _build_processor
    from mailbot_v26.insights.auto_priority_quality_gate import GateResult

    sent: list[str] = []
    gate = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    inbound_processor = _build_processor(tmp_path, sent, gate)
    inbound_processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})
    assert "Неподтверждённые доставки" not in sent[-1]

    db_path = tmp_path / "knowledge.sqlite"
    emitter = ContractEventEmitter(db_path)
    now = datetime.now(timezone.utc)
    for email_id in (1, 2):
        emitter.emit(
            EventV1(
                event_type=EventType.EMAIL_RECEIVED,
                ts_utc=(now - timedelta(minutes=10)).timestamp(),
                account_id="account@example.com",
                entity_id=None,
                email_id=email_id,
                payload={},
            )
        )
    emitter.emit(
        EventV1(
            event_type=EventType.TELEGRAM_FAILED,
            ts_utc=(now - timedelta(minutes=9)).timestamp(),
            account_id="account@example.com",
            entity_id=None,
            email_id=1,
            payload={
                "delivered": False,
                "error": "delivery_unknown",
                "delivery_state": "unknown",
                "error_class": "ReadTimeout",
            },
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.TELEGRAM_DELIVERED,
            ts_utc=(now - timedelta(minutes=9)).timestamp(),
            account_id="account@example.com",
            entity_id=None,
            email_id=2,
            payload={"delivered": True},
        )
    )

    sla = compute_notification_sla(analytics=KnowledgeAnalytics(db_path), now=now)
    assert sla.unknown_24h == 1

    inbound_processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})
    assert "Неподтверждённые доставки в Telegram (24ч): 1" in sent[-1]


# --- single-instance lock -------------------------------------------------------


def test_second_instance_lock_is_refused_and_released(tmp_path) -> None:
    from mailbot_v26.system.instance_lock import (
        acquire_instance_lock,
        is_instance_running,
    )

    db_path = tmp_path / "data" / "mailbot.sqlite"
    assert is_instance_running(db_path) is False
    first = acquire_instance_lock(db_path)
    assert first is not None
    try:
        assert acquire_instance_lock(db_path) is None
        assert is_instance_running(db_path) is True
    finally:
        first.release()
    first.release()  # idempotent

    assert is_instance_running(db_path) is False
    again = acquire_instance_lock(db_path)
    assert again is not None
    again.release()
    assert (tmp_path / "data" / "mailbot.sqlite.lock").exists()


def test_lock_probe_errors_fail_closed(tmp_path, monkeypatch) -> None:
    from mailbot_v26.system import instance_lock

    def _broken_open(*_args, **_kwargs):
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(instance_lock.os, "open", _broken_open)
    with pytest.raises(instance_lock.InstanceLockError):
        instance_lock.acquire_instance_lock(tmp_path / "mailbot.sqlite")
    (tmp_path / "mailbot.sqlite.lock").write_bytes(b"")
    with pytest.raises(instance_lock.InstanceLockError):
        instance_lock.is_instance_running(tmp_path / "mailbot.sqlite")


def test_main_exits_2_when_another_instance_holds_the_lock(
    monkeypatch, tmp_path, capsys
) -> None:
    from mailbot_v26.system.instance_lock import acquire_instance_lock
    from mailbot_v26.tests.test_polling_loop import _build_imap, _install_common_patches

    monkeypatch.setattr(
        start_module, "ResilientIMAP", _build_imap({_ACCOUNT.login: [[]]})
    )
    _install_common_patches(monkeypatch, [_ACCOUNT], tmp_path)
    integrity_calls: list[str] = []
    monkeypatch.setattr(
        start_module, "_check_build_integrity", lambda: integrity_calls.append("x")
    )

    holder = acquire_instance_lock(tmp_path / "mailbot.sqlite")
    assert holder is not None
    try:
        with pytest.raises(SystemExit) as exit_info:
            start_module.main(max_cycles=1)
    finally:
        holder.release()

    assert exit_info.value.code == 2
    assert "Letterbot is already running" in capsys.readouterr().out
    assert integrity_calls == []

    # A normal run takes the lock and gives it back.
    start_module.main(max_cycles=1)
    assert integrity_calls == ["x"]
    released = acquire_instance_lock(tmp_path / "mailbot.sqlite")
    assert released is not None
    released.release()


def _backup_archive(tmp_path: Path) -> Path:
    archive = tmp_path / "backup.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("state.json", "{}")
    return archive


def test_restore_refuses_while_instance_lock_is_held(tmp_path, monkeypatch) -> None:
    from mailbot_v26.system.instance_lock import acquire_instance_lock
    from mailbot_v26.tools import restore

    monkeypatch.setattr(restore, "_IS_WINDOWS", False)
    archive = _backup_archive(tmp_path)
    root = tmp_path / "root"
    holder = acquire_instance_lock(root / "data" / "mailbot.sqlite")
    assert holder is not None
    try:
        with pytest.raises(restore.RestoreRefusedError):
            restore.restore_from_backup(archive, base_dir=root)
    finally:
        holder.release()
    assert not (root / "state.json").exists()

    result = restore.restore_from_backup(archive, base_dir=root)
    assert "state.json" in result.restored_files


def test_restore_refuses_when_lock_probe_fails(tmp_path, monkeypatch) -> None:
    from mailbot_v26.system.instance_lock import InstanceLockError
    from mailbot_v26.tools import restore

    def _probe_fails(_db_path):
        raise InstanceLockError("probe failed")

    monkeypatch.setattr(restore, "_IS_WINDOWS", False)
    monkeypatch.setattr(restore, "is_instance_running", _probe_fails)

    with pytest.raises(restore.RestoreRefusedError):
        restore.ensure_letterbot_stopped(db_path=tmp_path / "mailbot.sqlite")
