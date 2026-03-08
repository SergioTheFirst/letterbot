from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from types import SimpleNamespace

from mailbot_v26.behavior.threading import compute_thread_key
from mailbot_v26.config.deadlock_policy import DeadlockPolicyConfig
from mailbot_v26.events.contract import EventType
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup_processor(monkeypatch, db_path) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", KnowledgeDB(db_path))
    monkeypatch.setattr(
        processor, "contract_event_emitter", ContractEventEmitter(db_path)
    )
    monkeypatch.setattr(
        processor, "event_emitter", SimpleNamespace(emit=lambda **_k: None)
    )
    monkeypatch.setattr(processor, "_check_crm_available", lambda: True)
    monkeypatch.setattr(
        processor, "decision_trace_writer", SimpleNamespace(write=lambda **_k: None)
    )
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **_kwargs: DeliveryResult(delivered=True, retryable=False),
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
            ENABLE_PRIORITY_V2=False,
            ENABLE_HIERARCHICAL_MAIL_TYPES=False,
            ENABLE_COMMITMENT_TRACKER=False,
            ENABLE_NARRATIVE_BINDING=False,
            ENABLE_NARRATIVE_PATTERNS=False,
            ENABLE_ANOMALY_ALERTS=False,
            ENABLE_CIRCADIAN_DELIVERY=False,
            ENABLE_ATTENTION_DEBT=False,
            ENABLE_SURPRISE_BUDGET="shadow",
            ENABLE_SILENCE_AS_SIGNAL="shadow",
            ENABLE_DEADLOCK_DETECTION="shadow",
        ),
    )
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: (llm_priority, None),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [],
    )
    monkeypatch.setattr(
        processor.context_store, "resolve_sender_entity", lambda **_k: None
    )
    monkeypatch.setattr(
        processor.context_store,
        "record_interaction_event",
        lambda **_k: (None, None),
    )
    monkeypatch.setattr(
        processor.context_store,
        "recompute_email_frequency",
        lambda **_k: (0.0, 0),
    )
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_a, **_k: SimpleNamespace(
            entropy=1.0,
            printable_ratio=1.0,
            quality_score=1.0,
            is_usable=True,
            reason="ok",
        ),
    )
    monkeypatch.setattr(
        processor,
        "apply_attention_gate",
        lambda *_a, **_k: SimpleNamespace(deferred=False, reason="test"),
    )
    monkeypatch.setattr(
        processor,
        "deadlock_policy",
        DeadlockPolicyConfig(window_days=5, min_messages=2, cooldown_hours=24),
    )


def test_deadlock_event_emitted_on_premium_path(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "premium.sqlite"
    _setup_processor(monkeypatch, db_path)
    now = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
    thread_key = compute_thread_key(
        account_email="account@example.com",
        rfc_message_id="<msg@example.com>",
        in_reply_to="<root@example.com>",
        references=None,
        subject="Re: Hello",
        from_email="sender@example.com",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, thread_key, received_at)
            VALUES (?, ?, ?)
            """,
            ("account@example.com", thread_key, (now - timedelta(days=1)).isoformat()),
        )
        conn.commit()

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        from_name=None,
        subject="Re: Hello",
        received_at=now,
        body_text="Hello",
        attachments=[],
        telegram_chat_id="chat-id",
        rfc_message_id="<msg@example.com>",
        in_reply_to="<root@example.com>",
        references=None,
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.DEADLOCK_DETECTED.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(row[0])
    assert payload["thread_key"] == thread_key
