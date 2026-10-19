from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.insights.quality_metrics import QualityMetricsSnapshot
from mailbot_v26.pipeline import daily_digest, digest_scheduler, weekly_digest
from mailbot_v26.tests.test_digest_scheduler import (
    DummyLogger,
    _build_config,
    _build_storage,
    _seed_deferred_email,
)
from mailbot_v26.worker.telegram_sender import DeliveryResult

_MONDAY_DUE = datetime(2025, 12, 29, 9, 0, tzinfo=timezone.utc)  # ISO 2026-W01


def _write_config(path: Path, *, daily: bool, weekly: bool) -> None:
    path.write_text(
        "[features]\n"
        f"enable_daily_digest = {str(daily).lower()}\n"
        f"enable_weekly_digest = {str(weekly).lower()}\n"
        "enable_quality_metrics = true\n"
        "enable_silence_as_signal = shadow\n"
        "\n"
        "[daily_digest]\n"
        "hour = 9\n"
        "minute = 0\n"
        "\n"
        "[weekly_digest]\n"
        "weekday = mon\n"
        "hour = 9\n"
        "minute = 0\n",
        encoding="utf-8",
    )


def _setup(monkeypatch, tmp_path: Path, *, daily: bool, weekly: bool):
    config_path = tmp_path / "config.ini"
    _write_config(config_path, daily=daily, weekly=weekly)
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)
    return _build_config(tmp_path), _build_storage(tmp_path)


class _Sender:
    def __init__(self) -> None:
        self.payloads: list[object] = []

    def __call__(self, payload) -> DeliveryResult:
        self.payloads.append(payload)
        return DeliveryResult(delivered=True, retryable=False)


def _tick(config, storage, sender, now: datetime, logger=None) -> None:
    digest_scheduler.run_digest_tick(
        now=now,
        config=config,
        storage=storage,
        telegram_sender=sender,
        logger=logger or DummyLogger(),
    )


def _count_calls(monkeypatch, module, name: str) -> list[int]:
    calls: list[int] = []
    original = getattr(module, name)

    def _wrapper(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, _wrapper)
    return calls


def _weekly_skip_reasons(storage) -> list[str]:
    with sqlite3.connect(storage.event_emitter.path) as conn:
        rows = conn.execute(
            "SELECT payload FROM events WHERE type = 'weekly_digest_skipped'"
        ).fetchall()
    return [str(json.loads(row[0]).get("reason")) for row in rows]


def _empty_weekly_data(**overrides) -> weekly_digest.WeeklyDigestData:
    fields: dict[str, object] = {
        "week_key": "2026-W01",
        "total_emails": 0,
        "deferred_emails": 0,
        "attention_entities": [],
        "commitment_counts": {"created": 0, "fulfilled": 0, "overdue": 0},
        "overdue_commitments": [],
        "trust_deltas": {"up": [], "down": []},
        "anomaly_alerts": [],
        "quality_metrics": QualityMetricsSnapshot(
            window_days=7,
            corrections_total=0,
            by_new_priority=[],
            by_engine=[],
            correction_rate=None,
            emails_received=0,
        ),
    }
    fields.update(overrides)
    return weekly_digest.WeeklyDigestData(**fields)


def test_has_weekly_content_false_for_zero_valued_structures() -> None:
    assert digest_scheduler._has_weekly_content(_empty_weekly_data()) is False


def test_has_weekly_content_true_for_real_signals() -> None:
    assert digest_scheduler._has_weekly_content(_empty_weekly_data(total_emails=3))
    assert digest_scheduler._has_weekly_content(
        _empty_weekly_data(commitment_counts={"created": 0, "overdue": 2})
    )
    assert digest_scheduler._has_weekly_content(
        _empty_weekly_data(trust_deltas={"up": [{"entity_id": "e"}], "down": []})
    )


def test_daily_quality_metrics_without_corrections_is_not_content(
    monkeypatch, tmp_path
) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    data = daily_digest._collect_digest_data(
        analytics=storage.analytics,
        account_email="account@example.com",
        include_quality_metrics=True,
        now=datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc),
    )

    assert data.quality_metrics is not None
    assert data.quality_metrics.corrections_total == 0
    assert daily_digest._has_digest_content(data) is False


def test_daily_no_mail_sends_no_digest(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    sender = _Sender()

    _tick(config, storage, sender, datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc))

    assert sender.payloads == []


def test_daily_no_content_is_evaluated_once_per_day(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    collect_calls = _count_calls(monkeypatch, daily_digest, "_collect_digest_data")
    scan_calls: list[int] = []
    monkeypatch.setattr(
        digest_scheduler, "run_silence_scan", lambda **_kwargs: scan_calls.append(1)
    )
    sender = _Sender()

    for hour, minute in ((9, 1), (9, 3), (17, 30), (23, 58)):
        _tick(
            config,
            storage,
            sender,
            datetime(2025, 1, 6, hour, minute, tzinfo=timezone.utc),
        )

    assert sender.payloads == []
    assert len(collect_calls) == 1
    assert len(scan_calls) == 1

    _tick(config, storage, sender, datetime(2025, 1, 7, 9, 1, tzinfo=timezone.utc))

    assert len(collect_calls) == 2
    assert sender.payloads == []


def test_daily_real_content_sends_exactly_once(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=True, weekly=False)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)
    sender = _Sender()

    for minute in (1, 3, 5):
        _tick(config, storage, sender, datetime(2025, 1, 6, 9, minute, tzinfo=timezone.utc))

    assert len(sender.payloads) == 1


def test_weekly_no_mail_sends_no_digest(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    sender = _Sender()

    _tick(config, storage, sender, _MONDAY_DUE)

    assert sender.payloads == []


def test_weekly_no_content_is_evaluated_once_per_week(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    collect_calls = _count_calls(monkeypatch, weekly_digest, "_collect_weekly_data")
    sender = _Sender()

    for hour, minute in ((9, 0), (9, 2), (15, 0)):
        _tick(config, storage, sender, _MONDAY_DUE.replace(hour=hour, minute=minute))

    assert sender.payloads == []
    assert len(collect_calls) == 1
    assert _weekly_skip_reasons(storage) == ["no_content"]


def test_weekly_not_due_skip_event_only_on_state_change(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    sender = _Sender()

    for day, hour, minute in ((30, 10, 0), (30, 10, 2), (31, 11, 0)):
        _tick(
            config,
            storage,
            sender,
            datetime(2025, 12, day, hour, minute, tzinfo=timezone.utc),
        )

    assert sender.payloads == []
    assert _weekly_skip_reasons(storage) == ["not_due"]


def test_weekly_real_content_sends_exactly_once(monkeypatch, tmp_path) -> None:
    config, storage = _setup(monkeypatch, tmp_path, daily=False, weekly=True)
    _seed_deferred_email(storage.knowledge_db, storage.contract_event_emitter)
    sender = _Sender()

    for minute in (0, 2, 4):
        _tick(config, storage, sender, _MONDAY_DUE.replace(minute=minute))

    assert len(sender.payloads) == 1
    assert _weekly_skip_reasons(storage) == ["already_sent"]
