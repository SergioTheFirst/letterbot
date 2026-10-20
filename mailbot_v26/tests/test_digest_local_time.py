"""B6: digests are scheduled in local time, with catch-up and no switch-day duplicate."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.tests._digest_helpers import (
    MSK,
    NEW_YORK_WINTER,
    ScriptedSender,
    account,
    bot_config,
    build_storage,
    emit,
    seed_deferred,
    tick,
    write_config,
)

LOGIN = "account@example.com"
HAWAII = timezone(timedelta(hours=-10))
START_PY = Path(__file__).resolve().parents[1] / "start.py"


def _setup(monkeypatch, tmp_path: Path, *, daily: bool, weekly: bool, **config):
    write_config(tmp_path, monkeypatch, daily=daily, weekly=weekly, **config)
    storage = build_storage(tmp_path)
    seed_deferred(storage, LOGIN)
    return bot_config(tmp_path, [account(LOGIN)]), storage


def _emit_daily_sent(storage, *, at: datetime) -> None:
    emit(
        storage,
        EventV1(
            event_type=EventType.DAILY_DIGEST_SENT,
            ts_utc=at.timestamp(),
            account_id=LOGIN,
            entity_id=None,
            email_id=0,
            payload={"account_emails": [LOGIN], "chat_scope": "tg:chat"},
        ),
    )


def test_worker_loop_passes_local_wall_clock_time_to_the_digest_tick() -> None:
    tree = ast.parse(START_PY.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "run_digest_tick"
    ]

    assert len(calls) == 1
    now_arg = next(kw.value for kw in calls[0].keywords if kw.arg == "now")
    # Aware local time: hour, weekday and day boundary follow the user's clock.
    assert ast.unparse(now_arg) == "datetime.now().astimezone()"


def test_daily_digest_goes_out_at_the_local_hour(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    sender = ScriptedSender()

    tick(config, storage, sender, datetime(2026, 10, 20, 8, 59, tzinfo=MSK))
    assert sender.payloads == []

    # 09:00 Moscow is 06:00 UTC: due by the local clock.
    tick(config, storage, sender, datetime(2026, 10, 20, 9, 0, tzinfo=MSK))
    assert len(sender.payloads) == 1


def test_naive_tick_time_is_read_as_local_time(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    sender = ScriptedSender()
    naive_local = datetime(2026, 10, 20, 9, 5)

    tick(config, storage, sender, naive_local)

    assert len(sender.payloads) == 1
    assert storage.knowledge_db.get_last_digest_sent_at(
        account_email=LOGIN
    ) == naive_local.astimezone(timezone.utc)


def test_already_sent_check_uses_the_local_calendar_day(tmp_path) -> None:
    storage = build_storage(tmp_path)
    # 02:30 Moscow on 20 Oct is still 19 Oct in UTC.
    _emit_daily_sent(storage, at=datetime(2026, 10, 19, 23, 30, tzinfo=timezone.utc))

    assert storage.analytics.has_daily_digest_sent(
        account_email=LOGIN, day=datetime(2026, 10, 20, 9, 5, tzinfo=MSK)
    )
    assert not storage.analytics.has_daily_digest_sent(
        account_email=LOGIN, day=datetime(2026, 10, 19, 23, 0, tzinfo=MSK)
    )


def test_new_local_day_starts_at_local_midnight(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    sender = ScriptedSender()

    for when in (
        datetime(2026, 10, 20, 9, 5, tzinfo=MSK),
        datetime(2026, 10, 20, 23, 59, tzinfo=MSK),
        # 00:30 Moscow on 21 Oct (still 20 Oct in UTC): a new day, not due yet.
        datetime(2026, 10, 21, 0, 30, tzinfo=MSK),
    ):
        tick(config, storage, sender, when)
    assert len(sender.payloads) == 1

    tick(config, storage, sender, datetime(2026, 10, 21, 9, 1, tzinfo=MSK))
    assert len(sender.payloads) == 2


def test_daily_digest_catches_up_after_the_worker_was_off_at_the_due_time(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    sender = ScriptedSender()

    # First tick after a restart at 14:10, long after the 09:00 slot.
    tick(config, storage, sender, datetime(2026, 10, 20, 14, 10, tzinfo=MSK))

    assert len(sender.payloads) == 1


@pytest.mark.parametrize(
    ("legacy_sent_at", "tick_at"),
    [
        # Old build sent at 09:00 UTC (12:00 Moscow); new build ticks at 14:00.
        (
            datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 20, 14, 0, tzinfo=MSK),
        ),
        # Old build sent at 09:00 UTC (04:00 New York); new build at 09:05 local.
        (
            datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 20, 9, 5, tzinfo=NEW_YORK_WINTER),
        ),
        # Old build sent at 09:00 UTC on 20 Oct, i.e. 23:00 on 19 Oct in
        # Hawaii; the new build ticks at 23:30 on that same local day.
        (
            datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 10, 19, 23, 30, tzinfo=HAWAII),
        ),
    ],
)
def test_switch_day_digest_sent_by_the_utc_build_is_not_repeated(
    monkeypatch, tmp_path, legacy_sent_at: datetime, tick_at: datetime
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    _emit_daily_sent(storage, at=legacy_sent_at)
    sender = ScriptedSender()

    logger = tick(config, storage, sender, tick_at)

    assert sender.payloads == []
    assert logger.checked("already_sent")


def test_weekly_digest_catches_up_later_in_the_same_iso_week(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    sender = ScriptedSender()

    # Slot is Monday 09:00; the worker first runs on Wednesday.
    tick(config, storage, sender, datetime(2026, 10, 21, 10, 0, tzinfo=MSK))
    assert len(sender.payloads) == 1
    assert sender.payloads[0].metadata["week_key"] == "2026-W43"

    for when in (
        datetime(2026, 10, 21, 10, 2, tzinfo=MSK),
        datetime(2026, 10, 25, 23, 59, tzinfo=MSK),
        # Next ISO week, before its slot.
        datetime(2026, 10, 26, 8, 59, tzinfo=MSK),
    ):
        tick(config, storage, sender, when)
    assert len(sender.payloads) == 1

    tick(config, storage, sender, datetime(2026, 10, 26, 9, 0, tzinfo=MSK))
    assert len(sender.payloads) == 2
    assert sender.payloads[1].metadata["week_key"] == "2026-W44"


def test_weekly_digest_with_a_late_week_slot_catches_up_after_the_weekend(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(
        monkeypatch,
        tmp_path,
        daily=False,
        weekly=True,
        weekly_weekday="fri",
        weekly_hour=17,
    )
    sender = ScriptedSender()

    # Slot was Friday 23 Oct 17:00 (ISO week 43); the PC was off until Monday.
    tick(config, storage, sender, datetime(2026, 10, 26, 9, 0, tzinfo=MSK))
    assert len(sender.payloads) == 1
    assert sender.payloads[0].metadata["week_key"] == "2026-W43"

    for when in (
        datetime(2026, 10, 28, 12, 0, tzinfo=MSK),
        # The next slot day starts a new digest week, not due before 17:00.
        datetime(2026, 10, 30, 16, 59, tzinfo=MSK),
    ):
        tick(config, storage, sender, when)
    assert len(sender.payloads) == 1

    tick(config, storage, sender, datetime(2026, 10, 30, 17, 0, tzinfo=MSK))
    assert len(sender.payloads) == 2
    assert sender.payloads[1].metadata["week_key"] == "2026-W44"


def test_weekly_digest_is_not_due_before_the_slot(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    sender = ScriptedSender()

    tick(config, storage, sender, datetime(2026, 10, 19, 8, 59, tzinfo=MSK))

    assert sender.payloads == []


def test_weekly_switch_day_digest_sent_by_the_utc_build_is_not_repeated(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    # The UTC build sent week 43 on Monday 19 Oct at 09:00 UTC (12:00 Moscow).
    emit(
        storage,
        EventV1(
            event_type=EventType.WEEKLY_DIGEST_SENT,
            ts_utc=datetime(2026, 10, 19, 9, 0, tzinfo=timezone.utc).timestamp(),
            account_id=LOGIN,
            entity_id=None,
            email_id=0,
            payload={"week_key": "2026-W43", "account_emails": [LOGIN]},
        ),
    )
    sender = ScriptedSender()

    logger = tick(config, storage, sender, datetime(2026, 10, 19, 14, 0, tzinfo=MSK))

    assert sender.payloads == []
    assert logger.checked("already_sent")


def test_digest_state_and_events_keep_utc_timestamps(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    now = datetime(2026, 10, 20, 9, 5, tzinfo=MSK)

    tick(config, storage, ScriptedSender(), now)

    sent_at = storage.knowledge_db.get_last_digest_sent_at(account_email=LOGIN)
    assert sent_at == now
    assert sent_at.utcoffset() == timezone.utc.utcoffset(None)
