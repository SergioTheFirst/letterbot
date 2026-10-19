from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mailbot_v26.observability.decision_trace import DecisionTraceWriter
from mailbot_v26.observability.metrics import MetricsAggregator
from mailbot_v26.tests.test_metrics_and_gates import _seed_metrics_db


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def executed(monkeypatch) -> list[str]:
    statements: list[str] = []
    original = MetricsAggregator._connect_readonly

    def _traced(self) -> sqlite3.Connection:
        conn = original(self)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(MetricsAggregator, "_connect_readonly", _traced)
    return statements


def _selects(statements: list[str]) -> int:
    return sum(1 for sql in statements if "SELECT" in sql.upper())


def test_snapshot_reuses_result_within_ttl(tmp_path: Path, executed) -> None:
    db_path = tmp_path / "metrics.sqlite"
    _seed_metrics_db(db_path)
    clock = _FakeClock()
    aggregator = MetricsAggregator(db_path, clock=clock)

    first = aggregator.snapshot()
    queries_after_first = _selects(executed)
    clock.now += aggregator.ttl_seconds - 1
    second = aggregator.snapshot()

    assert queries_after_first > 0
    assert _selects(executed) == queries_after_first
    assert second == first


def test_snapshot_refreshes_after_ttl(tmp_path: Path, executed) -> None:
    db_path = tmp_path / "metrics.sqlite"
    _seed_metrics_db(db_path)
    clock = _FakeClock()
    aggregator = MetricsAggregator(db_path, clock=clock)

    first = aggregator.snapshot()
    queries_after_first = _selects(executed)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO decision_traces (id, created_at, priority, shadow_priority,"
            " response_full) VALUES ('d5', datetime('now', '-1 days'), '🔵', '🔵', 'ok')"
        )
        conn.commit()
    clock.now += aggregator.ttl_seconds + 0.1
    second = aggregator.snapshot()

    assert _selects(executed) == 2 * queries_after_first
    assert second["days_7"]["shadow_accuracy"] > first["days_7"]["shadow_accuracy"]


def test_snapshot_ttl_is_short() -> None:
    aggregator = MetricsAggregator(Path("unused.sqlite"))

    assert 0 < aggregator.ttl_seconds <= 30


def test_failed_snapshot_is_not_cached(tmp_path: Path, monkeypatch) -> None:
    attempts: list[int] = []

    def _failing(self) -> sqlite3.Connection:
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(MetricsAggregator, "_connect_readonly", _failing)
    aggregator = MetricsAggregator(tmp_path / "missing.sqlite", clock=_FakeClock())

    first = aggregator.snapshot()
    second = aggregator.snapshot()

    assert first["days_7"]["shadow_accuracy"] == 0.0
    assert second == first
    assert len(attempts) == 4


def test_cached_snapshot_is_not_mutated_by_callers(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    _seed_metrics_db(db_path)
    aggregator = MetricsAggregator(db_path, clock=_FakeClock())

    first = aggregator.snapshot()
    first["days_7"]["shadow_accuracy"] = -1.0
    first.pop("days_30")
    second = aggregator.snapshot()

    assert round(second["days_7"]["shadow_accuracy"], 2) == 0.5
    assert "days_30" in second


def test_decision_traces_created_at_index_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "traces.sqlite"

    DecisionTraceWriter(db_path)

    with sqlite3.connect(db_path) as conn:
        indexes = {
            row[1]: row[2]
            for row in conn.execute("PRAGMA index_list('decision_traces')")
        }
        columns = [
            row[2]
            for row in conn.execute(
                "PRAGMA index_info('idx_decision_traces_created_at')"
            )
        ]

    assert "idx_decision_traces_created_at" in indexes
    assert columns == ["created_at"]
