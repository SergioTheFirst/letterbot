from __future__ import annotations

import json
import sqlite3
import time
import tracemalloc
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from mailbot_v26.budgets.contract import BudgetType
from mailbot_v26.budgets.gate import BudgetGate, BudgetGateConfig
from mailbot_v26.budgets.importance import (
    heuristic_importance,
    is_top_percentile,
    record_importance_score,
)
from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.llm.request_queue import LLMRequest, LLMRequestQueue

MAX_TEST_SECONDS = 1.0
MAX_TEST_MEMORY_BYTES = 256 * 1024 * 1024


@contextmanager
def resource_guard() -> None:
    start = time.perf_counter()
    tracemalloc.start()
    try:
        yield
    finally:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        elapsed = time.perf_counter() - start
        assert peak < MAX_TEST_MEMORY_BYTES
        assert elapsed < MAX_TEST_SECONDS


def _schema_sql() -> str:
    schema_path = Path(__file__).resolve().parents[1] / "storage" / "schema.sql"
    return schema_path.read_text(encoding="utf-8")


def _connect_memory() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(_schema_sql())
    return conn


def _connection_factory(conn: sqlite3.Connection) -> Callable[[], sqlite3.Connection]:
    return lambda: conn


def _log_test_event(conn: sqlite3.Connection, test_name: str, passed: bool) -> None:
    event = EventV1(
        event_type=EventType.ANOMALY_DETECTED,
        ts_utc=datetime.now(timezone.utc).timestamp(),
        account_id="test",
        entity_id=None,
        email_id=None,
        payload={"test": test_name, "passed": passed},
    )
    payload_json = json.dumps(event.payload, ensure_ascii=False)
    conn.execute(
        """
        INSERT OR IGNORE INTO events_v1 (
            event_type,
            ts_utc,
            ts,
            account_id,
            entity_id,
            email_id,
            payload,
            payload_json,
            schema_version,
            fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_type.value,
            event.ts_utc,
            datetime.fromtimestamp(event.ts_utc, tz=timezone.utc).isoformat(),
            event.account_id,
            event.entity_id,
            event.email_id,
            payload_json,
            payload_json,
            event.schema_version,
            fingerprint(event),
        ),
    )
    conn.commit()


def _assert_deterministic(fn: Callable[[], object]) -> object:
    results = [fn() for _ in range(10)]
    assert all(result == results[0] for result in results)
    return results[0]


def test_budget_gate_available_tokens() -> None:
    with resource_guard():

        def run() -> bool:
            conn = _connect_memory()
            gate = BudgetGate(
                Path(":memory:"),
                BudgetGateConfig(),
                emitter=None,
                connection_factory=_connection_factory(conn),
            )
            result = gate.can_use_llm("user@example.com")
            _log_test_event(conn, "test_budget_gate_available_tokens", result)
            conn.close()
            return result

        result = _assert_deterministic(run)
        assert result is True


def test_budget_gate_exhausted_tokens() -> None:
    with resource_guard():

        def run() -> bool:
            conn = _connect_memory()
            gate = BudgetGate(
                Path(":memory:"),
                BudgetGateConfig(default_llm_budget_tokens_per_year=5),
                emitter=None,
                connection_factory=_connection_factory(conn),
            )
            account = "user@example.com"
            gate.can_use_llm(account)
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                INSERT INTO budget_consumption (
                    account_email, budget_type, consumed, reason, event_id, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (account, "llm_tokens", 5, "test", "event_1", now),
            )
            conn.commit()
            result = gate.can_use_llm(account)
            _log_test_event(conn, "test_budget_gate_exhausted_tokens", result)
            conn.close()
            return result

        result = _assert_deterministic(run)
        assert result is False


def test_heuristic_importance_deterministic() -> None:
    with resource_guard():

        def run() -> tuple[int, tuple[str, ...]]:
            score = heuristic_importance(
                subject="Срочно: счет",
                body_text="Оплата до завтра.",
                from_email="billing@example.com",
                attachments=[{"filename": "invoice.pdf", "text": ""}],
            )
            return score.score, score.reasons

        result = _assert_deterministic(run)
        conn = _connect_memory()
        _log_test_event(conn, "test_heuristic_importance_deterministic", True)
        conn.close()
        assert isinstance(result, tuple)


def test_percentile_selection_top_20_percent() -> None:
    with resource_guard():

        def run() -> bool:
            conn = _connect_memory()
            account = "user@example.com"
            now = datetime.now(timezone.utc)
            for idx, score in enumerate([10, 20, 30, 40, 90], start=1):
                record_importance_score(
                    db_path=Path(":memory:"),
                    account_email=account,
                    email_id=idx,
                    score=score,
                    occurred_at=now,
                    connection_factory=_connection_factory(conn),
                )
            result = is_top_percentile(
                db_path=Path(":memory:"),
                account_email=account,
                current_score=90,
                percentile_threshold=80,
                window_days=7,
                received_at=now,
                connection_factory=_connection_factory(conn),
            )
            _log_test_event(
                conn,
                "test_percentile_selection_top_20_percent",
                {"is_top": result.is_top, "anchored": result.anchored},
            )
            conn.close()
            return result.is_top and result.anchored

        result = _assert_deterministic(run)
        assert result is True


def test_queue_enqueue_synchronous_timeout() -> None:
    with resource_guard():

        def run() -> bool:
            queue = LLMRequestQueue(max_size=1)
            request = LLMRequest(
                account_email="user@example.com",
                email_id=1,
                subject="Hello",
                from_email="sender@example.com",
                body_text="Body",
                attachments=[],
                received_at=datetime.now(timezone.utc),
                input_chars=10,
            )
            first = queue.enqueue(request, timeout_sec=0.01)
            second = queue.enqueue(request, timeout_sec=0.01)
            return first and not second

        result = _assert_deterministic(run)
        conn = _connect_memory()
        _log_test_event(conn, "test_queue_enqueue_synchronous_timeout", result)
        conn.close()
        assert result is True


def test_queue_full_fallback_to_heuristic() -> None:
    with resource_guard():

        def run() -> str:
            queue = LLMRequestQueue(max_size=1)
            request = LLMRequest(
                account_email="user@example.com",
                email_id=1,
                subject="Hello",
                from_email="sender@example.com",
                body_text="Body",
                attachments=[],
                received_at=datetime.now(timezone.utc),
                input_chars=10,
            )
            queue.enqueue(request, timeout_sec=0.01)
            queued = queue.enqueue(request, timeout_sec=0.01)
            priority = "heuristic" if not queued else "llm"
            return priority

        result = _assert_deterministic(run)
        conn = _connect_memory()
        _log_test_event(
            conn, "test_queue_full_fallback_to_heuristic", result == "heuristic"
        )
        conn.close()
        assert result == "heuristic"


def test_budget_consumed_event_logged() -> None:
    with resource_guard():

        def run() -> bool:
            conn = _connect_memory()
            gate = BudgetGate(
                Path(":memory:"),
                BudgetGateConfig(),
                emitter=None,
                connection_factory=_connection_factory(conn),
            )
            account = "user@example.com"
            gate.can_use_llm(account)
            gate.consume_budget(
                account_email=account,
                budget_type=BudgetType.LLM_TOKENS,
                amount=150,
                reason="llm_call:test",
            )
            row = conn.execute(
                "SELECT event_type FROM events_v1 WHERE event_type = ?",
                (EventType.BUDGET_CONSUMED.value,),
            ).fetchone()
            result = row is not None
            _log_test_event(conn, "test_budget_consumed_event_logged", result)
            conn.close()
            return result

        result = _assert_deterministic(run)
        assert result is True


def test_graceful_degradation_db_error() -> None:
    with resource_guard():

        def run() -> bool:
            conn = _connect_memory()
            conn.close()
            gate = BudgetGate(
                Path(":memory:"),
                BudgetGateConfig(),
                emitter=None,
                connection_factory=_connection_factory(conn),
            )
            return gate.can_use_llm("user@example.com")

        result = _assert_deterministic(run)
        conn = _connect_memory()
        _log_test_event(conn, "test_graceful_degradation_db_error", result is False)
        conn.close()
        assert result is False
