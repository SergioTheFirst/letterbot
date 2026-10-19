"""WP1c / A19: the delivery state is checked right after a queue item is claimed.

Before any context work, an item for an email whose delivery is already
reserved finishes: confirmed -> done without sending; unconfirmed (crash
between the reservation and the POST) -> DELIVERY_UNKNOWN. Neither is
dead-lettered nor gets a "delivery failed" notice. A missing context goes
through ``_rebuild_pipeline_context`` (a stub until WP2b fills it).
"""

from __future__ import annotations

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.pipeline import (
    PIPELINE_CACHE,
    PIPELINE_INBOUND_CACHE,
    PIPELINE_RAW_CACHE,
)
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.events.contract import EventType
from mailbot_v26.tests.test_delivery_invariants import (  # noqa: F401 - fixtures
    _ACCOUNT,
    _email_row,
    _fail_open_processor,
    _isolated_runtime,
    _queue_rows,
    _seed_context,
    _seed_email,
    _set_attempts,
    _setup,
    _unknown_contract_events,
    events,
    telegram,
)


def _restart_without_context(config) -> Storage:
    PIPELINE_CACHE.clear()
    PIPELINE_INBOUND_CACHE.clear()
    PIPELINE_RAW_CACHE.clear()
    return Storage(config.storage.db_path)


def _dead_letter_events(events) -> list:
    return [
        event
        for event in events.contract
        if event.event_type == EventType.IMAP_HEALTH
        and event.payload.get("subtype") == "dead_letter"
    ]


@pytest.mark.parametrize("stage", ["TG", "PARSE", "LLM"])
def test_restarted_item_for_delivered_email_finishes_without_sending(
    tmp_path, monkeypatch, telegram, events, stage
) -> None:
    config, storage, _legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    start_module._process_queue(storage, config, _fail_open_processor(), flags)
    assert len(telegram.accepted) == 1
    status_before = _email_row(storage, email_id)[0]

    restarted = _restart_without_context(config)
    restarted.enqueue_stage(email_id, stage)
    # Last attempt: without the check, a missing context would dead-letter it.
    _set_attempts(restarted, email_id, start_module.MAX_TELEGRAM_STAGE_ATTEMPTS - 1)
    start_module._process_queue(restarted, config, _fail_open_processor(), flags)

    assert len(telegram.accepted) == 1
    assert telegram.notices() == []
    assert _queue_rows(restarted) == []
    status, _, delivered_at = _email_row(restarted, email_id)
    assert status == status_before
    assert delivered_at
    assert _dead_letter_events(events) == []
    assert any(
        item.get("type") == "telegram_delivery_skipped_duplicate"
        and item.get("email_id") == email_id
        and item.get("payload", {}).get("stage") == stage
        for item in events.legacy
    )


def test_restarted_item_with_unconfirmed_reservation_becomes_delivery_unknown(
    tmp_path, monkeypatch, telegram, events
) -> None:
    config, storage, _legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    # Crash between the reservation commit and the POST.
    assert (
        storage.reserve_telegram_delivery(
            delivery_key=f"email:{email_id}",
            email_id=email_id,
            account_id=_ACCOUNT.login,
            chat_id="chat",
            kind="email",
        )
        == "reserved"
    )

    restarted = _restart_without_context(config)
    _set_attempts(restarted, email_id, start_module.MAX_TELEGRAM_STAGE_ATTEMPTS - 1)
    start_module._process_queue(restarted, config, _fail_open_processor(), flags)

    assert telegram.calls == 0
    status, error_last, _ = _email_row(restarted, email_id)
    assert status == "DELIVERY_UNKNOWN"
    assert error_last == "delivery_unknown:UnconfirmedReservation"
    assert _queue_rows(restarted) == []
    assert _dead_letter_events(events) == []
    unknown = _unknown_contract_events(events)
    assert len(unknown) == 1
    assert unknown[0].account_id == _ACCOUNT.login
    assert unknown[0].email_id == email_id
    assert unknown[0].payload["error"] == "delivery_unknown"
    assert unknown[0].payload["error_class"] == "UnconfirmedReservation"
    assert unknown[0].payload["delivered"] is False
    assert any(
        item.get("type") == "telegram_delivery_failed"
        and item.get("payload", {}).get("delivery_state") == "unknown"
        for item in events.legacy
    )


def test_settle_write_failure_in_claim_check_retries_without_sending(
    tmp_path, monkeypatch, telegram, events
) -> None:
    import sqlite3

    config, storage, _legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    start_module._process_queue(storage, config, _fail_open_processor(), flags)
    assert len(telegram.accepted) == 1

    restarted = _restart_without_context(config)
    restarted.enqueue_stage(email_id, "TG")
    _set_attempts(restarted, email_id, start_module.MAX_TELEGRAM_STAGE_ATTEMPTS - 1)
    real_mark_done = restarted.mark_done
    failures = {"left": 1}

    def flaky_mark_done(queue_id: int) -> None:
        if failures["left"]:
            failures["left"] -= 1
            raise sqlite3.OperationalError("database is locked")
        real_mark_done(queue_id)

    monkeypatch.setattr(restarted, "mark_done", flaky_mark_done)
    start_module._process_queue(restarted, config, _fail_open_processor(), flags)

    # Not dead-lettered, no notice: the item waits for its retry.
    assert len(telegram.accepted) == 1
    assert telegram.notices() == []
    rows = _queue_rows(restarted)
    assert len(rows) == 1
    assert "database is locked" in (rows[0][2] or "")
    assert _email_row(restarted, email_id)[0] != "DELIVERY_FAILED"

    with restarted.conn:
        restarted.conn.execute("UPDATE queue SET not_before = NULL")
    start_module._process_queue(restarted, config, _fail_open_processor(), flags)

    assert len(telegram.accepted) == 1
    assert _queue_rows(restarted) == []


def test_unreadable_delivery_state_falls_through_to_the_guarded_path(
    tmp_path, monkeypatch, telegram, events, caplog
) -> None:
    import sqlite3

    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    real_state = storage.get_telegram_delivery_state
    calls = {"n": 0}

    def flaky_state(delivery_key, email_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("disk I/O error")
        return real_state(delivery_key, email_id)

    monkeypatch.setattr(storage, "get_telegram_delivery_state", flaky_state)
    with caplog.at_level("INFO"):
        start_module._process_queue(storage, config, legacy_processor, flags)

    assert "telegram_delivery_claim_check_failed" in caplog.text
    assert len(telegram.accepted) == 1
    assert _queue_rows(storage) == []
    assert _email_row(storage, email_id)[2]


def test_missing_context_goes_through_the_rebuild_hook(
    tmp_path, monkeypatch, telegram, events
) -> None:
    config, storage, legacy_processor, flags = _setup(
        tmp_path, monkeypatch, premium=False
    )
    email_id = _seed_email(storage, stage="TG")
    PIPELINE_CACHE.clear()
    calls: list[dict] = []

    def fake_rebuild(*, storage, config, email_id, stage):
        calls.append({"email_id": email_id, "stage": stage})
        _seed_context(email_id, stage=stage)
        return PIPELINE_CACHE[email_id]

    monkeypatch.setattr(start_module, "_rebuild_pipeline_context", fake_rebuild)
    start_module._process_queue(storage, config, legacy_processor, flags)

    assert calls == [{"email_id": email_id, "stage": "TG"}]
    assert len(telegram.accepted) == 1
    assert _queue_rows(storage) == []


def test_rebuild_pipeline_context_stub_is_keyword_only_and_returns_none(
    tmp_path,
) -> None:
    storage = Storage(tmp_path / "db.sqlite")
    assert (
        start_module._rebuild_pipeline_context(
            storage=storage, config=None, email_id=1, stage="TG"
        )
        is None
    )
    with pytest.raises(TypeError):
        start_module._rebuild_pipeline_context(storage, None, 1, "TG")  # type: ignore[misc]
