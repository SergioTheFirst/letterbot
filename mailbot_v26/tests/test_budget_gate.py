from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from mailbot_v26.budgets.consumer import BudgetConsumer
from mailbot_v26.budgets.contract import BudgetType
from mailbot_v26.budgets.gate import BudgetGate, BudgetGateConfig
from mailbot_v26.budgets.importance import (
    heuristic_importance,
    is_top_percentile,
    record_importance_score,
)
from mailbot_v26.llm.request_queue import LLMRequest, LLMRequestQueue
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "budget.sqlite"
    KnowledgeDB(db_path)
    return db_path


def test_budget_gate_creates_yearly_default(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    gate = BudgetGate(db_path, BudgetGateConfig())
    assert gate.can_use_llm("user@example.com") is True

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT limit_value, period FROM account_budgets WHERE account_email = ?",
            ("user@example.com",),
        ).fetchone()
    assert row is not None
    assert row[0] == 900000
    assert row[1] == "YEARLY"


def test_budget_gate_exhausted_denies(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    gate = BudgetGate(db_path, BudgetGateConfig())
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO account_budgets (
                account_email, budget_type, limit_value, period, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "llm_tokens", 10, "YEARLY", now, now),
        )
        for i in range(10):
            conn.execute(
                """
                INSERT INTO budget_consumption (
                    account_email, budget_type, consumed, reason, event_id, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("user@example.com", "llm_tokens", 1, "test", f"evt_{i}", now),
            )
        conn.commit()

    assert gate.can_use_llm("user@example.com") is False


def test_budget_consume_tokens(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    gate = BudgetGate(db_path, BudgetGateConfig())
    assert gate.consume_budget(
        "user@example.com", BudgetType.LLM_TOKENS, 150, reason="llm_call"
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT consumed, reason
            FROM budget_consumption
            WHERE account_email = ?
            """,
            ("user@example.com",),
        ).fetchone()
    assert row == (150, "llm_call")


def test_budget_gate_db_error_fails_safe(tmp_path: Path) -> None:
    bad_path = tmp_path / "dir"
    bad_path.mkdir()
    gate = BudgetGate(bad_path, BudgetGateConfig())
    assert gate.can_use_llm("user@example.com") is False


def test_heuristic_importance_deterministic() -> None:
    first = heuristic_importance(
        subject="Срочно оплатить счет",
        body_text="Пожалуйста, оплатите до завтра.",
        from_email="billing@example.com",
        attachments=[{"filename": "invoice.pdf"}],
    )
    second = heuristic_importance(
        subject="Срочно оплатить счет",
        body_text="Пожалуйста, оплатите до завтра.",
        from_email="billing@example.com",
        attachments=[{"filename": "invoice.pdf"}],
    )
    assert first.score == second.score
    assert first.reasons == second.reasons


def test_importance_record_and_percentile(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    now = datetime.now(timezone.utc)
    scores = [10, 20, 30, 40, 50]
    for idx, score in enumerate(scores, start=1):
        record_importance_score(
            db_path=db_path,
            account_email="user@example.com",
            email_id=idx,
            score=score,
            occurred_at=now,
        )
    assert is_top_percentile(
        db_path=db_path,
        account_email="user@example.com",
        current_score=50,
        percentile_threshold=80,
        window_days=7,
    )
    assert not is_top_percentile(
        db_path=db_path,
        account_email="user@example.com",
        current_score=10,
        percentile_threshold=80,
        window_days=7,
    )


def test_llm_queue_enqueue_timeout() -> None:
    queue = LLMRequestQueue(max_size=1)
    request = LLMRequest(
        account_email="user@example.com",
        email_id=1,
        subject="Hello",
        from_email="sender@example.com",
        body_text="Body",
        attachments=[],
        received_at=datetime.now(timezone.utc),
        input_chars=100,
    )
    assert queue.enqueue(request, timeout_sec=0.0) is True
    assert queue.enqueue(request, timeout_sec=0.0) is False


def test_budget_consumer_estimates_tokens(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    gate = BudgetGate(db_path, BudgetGateConfig())
    consumer = BudgetConsumer(gate)
    assert consumer.on_llm_call(
        account_email="user@example.com",
        tokens_used=None,
        input_chars=400,
        model="gigachat",
        success=True,
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT consumed
            FROM budget_consumption
            WHERE account_email = ?
            """,
            ("user@example.com",),
        ).fetchone()
    assert row == (100,)
