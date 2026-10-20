"""B14: a due digest that policy denies is not re-collected on every tick, and
digest_tick_checked is logged when the decision changes, not on every tick."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest

from mailbot_v26.pipeline import daily_digest, digest_scheduler, weekly_digest
from mailbot_v26.system_health import OperationalMode
from mailbot_v26.tests._digest_helpers import (
    MSK,
    ScriptedSender,
    account,
    bot_config,
    build_storage,
    seed_deferred,
    tick,
    write_config,
)

LOGIN = "account@example.com"
# Monday 19 Oct 2026, Moscow time: both digests are due from 09:00.
TICKS_DENIED = [datetime(2026, 10, 19, 10, minute, tzinfo=MSK) for minute in (0, 2, 4)]
TICK_ALLOWED = datetime(2026, 10, 19, 10, 6, tzinfo=MSK)
COLLECTORS = {
    "daily": (daily_digest, "_collect_digest_data"),
    "weekly": (weekly_digest, "_collect_weekly_data"),
}


@dataclass
class _Policy:
    allowed: bool = False
    calls: int = 0

    def __call__(self, **_kwargs):
        self.calls += 1
        return _Decision(self.allowed)


@dataclass(frozen=True)
class _Decision:
    allowed: bool
    mode: OperationalMode = OperationalMode.DEGRADED_NO_TELEGRAM

    @property
    def allow_daily_digest(self) -> bool:
        return self.allowed

    @property
    def allow_weekly_digest(self) -> bool:
        return self.allowed


def _setup(monkeypatch, tmp_path, kind: str, accounts=None):
    write_config(
        tmp_path, monkeypatch, daily=kind == "daily", weekly=kind == "weekly"
    )
    storage = build_storage(tmp_path)
    seed_deferred(storage, LOGIN)
    config = bot_config(tmp_path, accounts or [account(LOGIN)])
    return config, storage


def _count_calls(monkeypatch, module, name: str) -> list[int]:
    calls: list[int] = []
    real = getattr(module, name)

    def _wrapper(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(module, name, _wrapper)
    return calls


def _checked(logger, **match) -> list[dict[str, object]]:
    return [
        fields
        for _level, name, fields in logger.events
        if name == "digest_tick_checked"
        and all(fields.get(key) == value for key, value in match.items())
    ]


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_policy_denied_digest_is_not_recollected_on_every_tick(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    policy = _Policy(allowed=False)
    monkeypatch.setattr(digest_scheduler, "_evaluate_policy", policy)
    collected = _count_calls(monkeypatch, *COLLECTORS[kind])
    scans = _count_calls(monkeypatch, digest_scheduler, "run_silence_scan")
    sender = ScriptedSender()
    logger = None

    for when in TICKS_DENIED:
        logger = tick(config, storage, sender, when, logger)

    assert sender.payloads == []
    assert collected == []
    assert scans == []
    assert len(_checked(logger, digest_type=kind, reason="policy_denied")) == 1

    # Telegram is back: the digest goes out in the same window.
    policy.allowed = True
    tick(config, storage, sender, TICK_ALLOWED, logger)

    assert len(sender.payloads) == 1
    assert len(collected) == 1


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_tick_checked_is_logged_when_the_decision_changes(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    sender = ScriptedSender()
    logger = None

    for minute in (0, 2, 4):  # 08:xx, before the 09:00 slot
        logger = tick(
            config, storage, sender, datetime(2026, 10, 19, 8, minute, tzinfo=MSK), logger
        )
    for minute in (1, 3, 5):
        tick(config, storage, sender, datetime(2026, 10, 19, 9, minute, tzinfo=MSK), logger)

    assert len(sender.payloads) == 1
    reasons = [
        fields.get("reason") or fields.get("decision")
        for fields in _checked(logger, digest_type=kind)
    ]
    assert reasons == ["not_due", "due", "already_sent"]


def test_missing_chat_id_is_warned_once_per_digest_type(monkeypatch, tmp_path) -> None:
    write_config(tmp_path, monkeypatch, daily=True, weekly=True)
    storage = build_storage(tmp_path)
    config = bot_config(tmp_path, [account(LOGIN, chat_id="")])
    logger = None

    for when in TICKS_DENIED:
        logger = tick(config, storage, ScriptedSender(), when, logger)

    warnings = [
        fields["digest_type"]
        for level, name, fields in logger.events
        if level == "warning" and name == "digest_tick_checked"
    ]
    assert warnings == ["daily", "weekly"]


def test_skips_of_a_secondary_chat_account_are_logged_once(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(
        monkeypatch,
        tmp_path,
        "daily",
        accounts=[account(LOGIN), account("second@example.com")],
    )
    logger = None

    for when in TICKS_DENIED:
        logger = tick(config, storage, ScriptedSender(), when, logger)

    assert (
        len(
            _checked(
                logger,
                account_email="second@example.com",
                reason="dedup_chat_id_already_processed",
            )
        )
        == 1
    )
