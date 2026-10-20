"""B17: a digest goes out at most once per window, and a failed read never
counts as "not sent yet"."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime

import pytest

from mailbot_v26.pipeline import digest_scheduler
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.tests._digest_helpers import (
    MSK,
    ScriptedSender,
    account,
    bot_config,
    build_storage,
    daily_sent_key,
    override_value,
    seed_deferred,
    tick,
    weekly_sent_key,
    write_config,
)
from mailbot_v26.worker.telegram_sender import DeliveryResult

LOGIN = "account@example.com"
OTHER = "other@example.com"
# Monday 19 Oct 2026 10:00 Moscow: daily and weekly digests are both due.
DUE = datetime(2026, 10, 19, 10, 0, tzinfo=MSK)
LATER_SAME_DAY = datetime(2026, 10, 19, 10, 2, tzinfo=MSK)
NEXT_DAY = datetime(2026, 10, 20, 10, 0, tzinfo=MSK)
WINDOWS = {"daily": "2026-10-19", "weekly": "2026-W43"}

MAYBE_DELIVERED = DeliveryResult(
    delivered=False,
    retryable=False,
    error="delivery_unknown:ReadTimeout",
    maybe_delivered=True,
)
NOT_SENT_RETRY = DeliveryResult(
    delivered=False, retryable=True, error="connect timeout", status_code=None
)
REJECTED = DeliveryResult(
    delivered=False,
    retryable=False,
    error='{"ok":false,"error_code":400,"description":"Bad Request"}',
    status_code=400,
)


def _setup(monkeypatch, tmp_path, kind: str, accounts=(LOGIN,)):
    write_config(
        tmp_path, monkeypatch, daily=kind == "daily", weekly=kind == "weekly"
    )
    storage = build_storage(tmp_path)
    for login in accounts:
        seed_deferred(storage, login)
    config = bot_config(
        tmp_path, [account(login, f"chat-{login[0]}") for login in accounts]
    )
    return config, storage


def _sent_key(kind: str, login: str = LOGIN) -> str:
    return daily_sent_key(login) if kind == "daily" else weekly_sent_key(login)


def _errors(logger, event: str) -> list[dict[str, object]]:
    return [
        fields
        for level, name, fields in logger.events
        if level == "error" and name == event
    ]


def _empty_database(tmp_path) -> KnowledgeAnalytics:
    path = tmp_path / "no-events.sqlite"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE unrelated (id INTEGER)")
    return KnowledgeAnalytics(path)


def test_sent_checks_raise_instead_of_reading_a_failure_as_not_sent(
    tmp_path,
) -> None:
    analytics = _empty_database(tmp_path)

    with pytest.raises(sqlite3.Error):
        analytics.has_daily_digest_sent(account_email=LOGIN, day=DUE)
    with pytest.raises(sqlite3.Error):
        analytics.has_weekly_digest_sent(account_email=LOGIN, week_key="2026-W43")


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_failed_sent_check_skips_only_that_digest_and_is_logged(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind, accounts=(LOGIN, OTHER))
    method = "has_daily_digest_sent" if kind == "daily" else "has_weekly_digest_sent"
    real_check = getattr(storage.analytics, method)

    def _check(*, account_email: str, **kwargs) -> bool:
        if account_email == LOGIN:
            raise sqlite3.OperationalError("database is locked")
        return real_check(account_email=account_email, **kwargs)

    monkeypatch.setattr(storage.analytics, method, _check)
    sender = ScriptedSender()

    logger = tick(config, storage, sender, DUE)

    # Fail closed for the account whose check failed; the other chat still
    # gets its digest.
    assert sender.chats == ["chat-o"]
    failures = _errors(logger, "digest_sent_check_failed")
    assert [(f["digest_type"], f["account_email"]) for f in failures] == [
        (kind, LOGIN)
    ]
    assert "database is locked" in str(failures[0]["error"])


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_unreadable_sent_marker_skips_the_digest(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)

    def _broken(*_args, **_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(digest_scheduler, "_read_sent_marker", _broken)
    sender = ScriptedSender()

    logger = tick(config, storage, sender, DUE)

    assert sender.payloads == []
    assert _errors(logger, "digest_sent_check_failed")


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_window_is_reserved_before_the_post(monkeypatch, tmp_path, kind: str) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    seen_at_post: list[str | None] = []

    def _sender(payload) -> DeliveryResult:
        seen_at_post.append(override_value(storage, _sent_key(kind)))
        return DeliveryResult(delivered=True, retryable=False)

    tick(config, storage, _sender, DUE)

    assert seen_at_post == [WINDOWS[kind]]
    assert override_value(storage, _sent_key(kind)) == WINDOWS[kind]


@pytest.mark.parametrize("kind", ["daily", "weekly"])
@pytest.mark.parametrize(
    "outcome",
    [MAYBE_DELIVERED, RuntimeError("socket closed after the request was sent")],
    ids=["accepted_then_timeout", "sender_raised"],
)
def test_digest_that_may_have_been_delivered_is_never_resent(
    monkeypatch, tmp_path, kind: str, outcome: object
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    sender = ScriptedSender(outcome)

    first = tick(config, storage, sender, DUE)
    tick(config, storage, sender, LATER_SAME_DAY)

    assert len(sender.payloads) == 1
    failure = _errors(first, "digest_failed")
    assert failure and failure[0]["delivery"] == "unknown"
    assert storage.knowledge_db.get_last_digest_sent_at(account_email=LOGIN) is None
    assert storage.knowledge_db.get_last_weekly_digest_key(account_email=LOGIN) is None


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_definite_rejection_is_not_retried_in_the_same_window(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    sender = ScriptedSender(REJECTED)

    first = tick(config, storage, sender, DUE)
    tick(config, storage, sender, LATER_SAME_DAY)

    assert len(sender.payloads) == 1
    assert _errors(first, "digest_failed")[0]["delivery"] == "failed"


def test_rejected_daily_digest_goes_out_again_the_next_day(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, "daily")
    sender = ScriptedSender(REJECTED)

    tick(config, storage, sender, DUE)
    tick(config, storage, sender, NEXT_DAY)

    assert len(sender.payloads) == 2
    assert override_value(storage, _sent_key("daily")) == "2026-10-20"


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_retryable_failure_gives_the_window_back(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)
    sender = ScriptedSender(NOT_SENT_RETRY)

    first = tick(config, storage, sender, DUE)
    assert override_value(storage, _sent_key(kind)) is None
    tick(config, storage, sender, LATER_SAME_DAY)
    tick(config, storage, sender, LATER_SAME_DAY)

    assert len(sender.payloads) == 2
    assert _errors(first, "digest_failed")[0]["delivery"] == "retryable"
    assert override_value(storage, _sent_key(kind)) == WINDOWS[kind]


@pytest.mark.parametrize("kind", ["daily", "weekly"])
def test_unavailable_reservation_store_sends_nothing(
    monkeypatch, tmp_path, kind: str
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, kind)

    def _broken(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(digest_scheduler, "_reserve_sent_marker", _broken)
    sender = ScriptedSender()

    logger = tick(config, storage, sender, DUE)

    assert sender.payloads == []
    assert _errors(logger, "digest_reserve_failed")
