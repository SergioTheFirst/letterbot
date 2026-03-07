from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.budgets import importance
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _configure_importance_db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE email_importance_scores (
            account_email TEXT NOT NULL,
            email_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            ts_utc REAL NOT NULL,
            PRIMARY KEY (account_email, email_id)
        )
        """
    )
    conn.commit()
    connection_factory = lambda: conn

    def _record_importance_score(**kwargs):
        kwargs.pop("db_path", None)
        return importance.record_importance_score(
            db_path=Path(":memory:"),
            connection_factory=connection_factory,
            **kwargs,
        )

    def _is_top_percentile(**kwargs):
        kwargs.pop("db_path", None)
        return importance.is_top_percentile(
            db_path=Path(":memory:"),
            connection_factory=connection_factory,
            **kwargs,
        )

    monkeypatch.setattr(processor, "record_importance_score", _record_importance_score)
    monkeypatch.setattr(processor, "is_top_percentile", _is_top_percentile)
    return conn, connection_factory


def _configure_minimal_pipeline(monkeypatch, llm_calls):
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Ответить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="gigachat",
    )

    def _fake_run_llm_stage(**_kwargs):
        llm_calls.append("called")
        return llm_result

    monkeypatch.setattr(processor, "run_llm_stage", _fake_run_llm_stage)
    monkeypatch.setattr(
        processor, "knowledge_db", SimpleNamespace(save_email=lambda **_kwargs: None)
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **_kwargs: DeliveryResult(delivered=True, retryable=False),
    )
    monkeypatch.setattr(
        processor,
        "budget_gate",
        SimpleNamespace(can_use_llm=lambda _account_email: True),
    )
    monkeypatch.setattr(
        processor,
        "budget_consumer",
        SimpleNamespace(on_llm_call=lambda **_kwargs: None),
    )


def test_deterministic_historical_gating(monkeypatch) -> None:
    conn, connection_factory = _configure_importance_db(monkeypatch)
    account = "account@example.com"
    anchor = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)

    for idx, score in enumerate([10, 20, 30, 40, 90], start=1):
        importance.record_importance_score(
            db_path=Path(":memory:"),
            account_email=account,
            email_id=idx,
            score=score,
            occurred_at=anchor - timedelta(days=1),
            connection_factory=connection_factory,
        )

    llm_calls: list[str] = []
    _configure_minimal_pipeline(monkeypatch, llm_calls)

    for _ in range(2):
        processor.process_message(
            account_email=account,
            message_id=10,
            from_email="sender@example.com",
            subject="Срочно: оплата",
            received_at=anchor,
            body_text="Оплата до завтра.",
            attachments=[{"filename": "invoice.pdf", "text": ""}],
            telegram_chat_id="chat",
        )

    conn.close()
    assert llm_calls == ["called", "called"]


def test_anchor_ignores_wall_clock_now(monkeypatch) -> None:
    conn, connection_factory = _configure_importance_db(monkeypatch)
    account = "user@example.com"
    anchor = datetime(2024, 2, 1, 9, 0, tzinfo=timezone.utc)

    for idx, score in enumerate([5, 15, 25, 35, 95], start=1):
        importance.record_importance_score(
            db_path=Path(":memory:"),
            account_email=account,
            email_id=idx,
            score=score,
            occurred_at=anchor - timedelta(days=2),
            connection_factory=connection_factory,
        )

    result_a = importance.is_top_percentile(
        db_path=Path(":memory:"),
        account_email=account,
        current_score=95,
        percentile_threshold=80,
        window_days=7,
        anchor_ts_utc=anchor.timestamp(),
        now=anchor + timedelta(days=365),
        connection_factory=connection_factory,
    )
    result_b = importance.is_top_percentile(
        db_path=Path(":memory:"),
        account_email=account,
        current_score=95,
        percentile_threshold=80,
        window_days=7,
        anchor_ts_utc=anchor.timestamp(),
        now=anchor + timedelta(days=730),
        connection_factory=connection_factory,
    )

    conn.close()
    assert result_a.is_top == result_b.is_top
    assert result_a.anchored is True
    assert result_b.anchored is True


def test_pipeline_resilient_to_percentile_failure(monkeypatch) -> None:
    llm_calls: list[str] = []
    _configure_minimal_pipeline(monkeypatch, llm_calls)

    def _fail_percentile(**_kwargs):
        raise sqlite3.OperationalError("db down")

    monkeypatch.setattr(processor, "is_top_percentile", _fail_percentile)
    monkeypatch.setattr(processor, "record_importance_score", lambda **_kwargs: None)

    delivered: list[int] = []

    def _enqueue_tg(*, email_id: int, payload):
        delivered.append(email_id)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=42,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert llm_calls == []
    assert delivered == [42]


def test_pipeline_allows_llm_for_cold_start_with_insufficient_history(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")
    llm_calls: list[str] = []
    _configure_minimal_pipeline(monkeypatch, llm_calls)

    monkeypatch.setattr(
        processor,
        "is_top_percentile",
        lambda **_kwargs: importance.PercentileGateResult(
            is_top=False,
            anchored=True,
            anchor_ts_utc=datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc).timestamp(),
        ),
    )
    monkeypatch.setattr(processor, "record_importance_score", lambda **_kwargs: None)
    monkeypatch.setattr(processor, "_count_recent_importance_history", lambda **_kwargs: 0)

    processor.process_message(
        account_email="account@example.com",
        message_id=77,
        from_email="sender@example.com",
        subject="Счёт №1234",
        received_at=datetime(2024, 3, 1, 10, 0, tzinfo=timezone.utc),
        body_text="Оплатить до 15.04.2026",
        attachments=[{"filename": "invoice.xlsx", "text": "Итого 87500"}],
        telegram_chat_id="chat",
    )

    assert llm_calls == ["called"]
    assert "llm_cold_start_percentile_allow" in caplog.text
