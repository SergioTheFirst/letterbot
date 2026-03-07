from __future__ import annotations

import sqlite3
from pathlib import Path

from mailbot_v26.observability.metrics import MetricsAggregator, SystemGates


def _seed_metrics_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE decision_traces (
                id TEXT PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                priority TEXT,
                shadow_priority TEXT,
                response_full TEXT
            );
            CREATE TABLE action_feedback (
                id TEXT PRIMARY KEY,
                decision TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE commitments (
                id INTEGER PRIMARY KEY,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            INSERT INTO decision_traces (id, created_at, priority, shadow_priority, response_full)
            VALUES
                ('d1', datetime('now', '-1 days'), '🔵', '🔵', 'ok'),
                ('d2', datetime('now', '-2 days'), '🔴', '🟡', ''),
                ('d3', datetime('now', '-10 days'), '🟡', '🟡', 'ok'),
                ('d4', datetime('now', '-40 days'), '🔵', '🔵', 'ok')
            """
        )
        conn.execute(
            """
            INSERT INTO action_feedback (id, decision, created_at)
            VALUES
                ('f1', 'accepted', datetime('now', '-1 days')),
                ('f2', 'rejected', datetime('now', '-1 days')),
                ('f3', 'accepted', datetime('now', '-5 days')),
                ('f4', 'accepted', datetime('now', '-20 days')),
                ('f5', 'accepted', datetime('now', '-40 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO commitments (id, status, created_at)
            VALUES
                (1, 'fulfilled', datetime('now', '-1 days')),
                (2, 'expired', datetime('now', '-2 days')),
                (3, 'fulfilled', datetime('now', '-5 days')),
                (4, 'fulfilled', datetime('now', '-20 days')),
                (5, 'expired', datetime('now', '-40 days'))
            """
        )
        conn.commit()


def test_metrics_aggregator_calculations(tmp_path: Path) -> None:
    db_path = tmp_path / "metrics.sqlite"
    _seed_metrics_db(db_path)
    aggregator = MetricsAggregator(db_path)
    snapshot = aggregator.snapshot()

    seven_day = snapshot["days_7"]
    thirty_day = snapshot["days_30"]

    assert round(seven_day["shadow_accuracy"], 2) == 0.5
    assert round(seven_day["preview_accept_rate"], 2) == 0.67
    assert round(seven_day["commitment_fulfillment_rate"], 2) == 0.67
    assert round(seven_day["llm_failure_rate"], 2) == 0.5

    assert round(thirty_day["shadow_accuracy"], 2) == 0.67
    assert round(thirty_day["preview_accept_rate"], 2) == 0.75
    assert round(thirty_day["commitment_fulfillment_rate"], 2) == 0.75
    assert round(thirty_day["llm_failure_rate"], 2) == 0.33


def test_system_gates_evaluation() -> None:
    metrics = {
        "days_30": {
            "shadow_accuracy": 0.4,
            "preview_accept_rate": 0.5,
            "commitment_fulfillment_rate": 0.6,
            "llm_failure_rate": 0.5,
        }
    }
    gates = SystemGates()
    evaluation = gates.evaluate(metrics)

    assert evaluation.passed is False
    assert set(evaluation.failed_reasons) == {
        "shadow_accuracy",
        "preview_accept_rate",
        "commitment_fulfillment_rate",
        "llm_failure_rate",
    }
