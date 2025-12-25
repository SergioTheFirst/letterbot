from __future__ import annotations

import sqlite3
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.insights.commitment_lifecycle import CommitmentStatusUpdate
from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.insights.trust_score import (
    TrustScoreComponents,
    TrustScoreResult,
    TrustSnapshot,
)
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.pipeline import processor
from mailbot_v26.storage.context_layer import EntityResolution
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _feature_flags() -> SimpleNamespace:
    return SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=False,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
        ENABLE_COMMITMENT_TRACKER=True,
    )


def _stub_llm_result():
    return SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary="Body summary",
        attachment_summaries=[],
    )


def _load_event_types(db_path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT type FROM events ORDER BY timestamp").fetchall()
    return [row[0] for row in rows]


def test_event_core_emits_expected_events(monkeypatch, tmp_path) -> None:
    emitter = EventEmitter(tmp_path / "events.sqlite")
    monkeypatch.setattr(processor, "event_emitter", emitter)
    monkeypatch.setattr(processor, "feature_flags", _feature_flags())
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _stub_llm_result())
    monkeypatch.setattr(processor.shadow_priority_engine, "compute", lambda *args, **kwargs: ("🔵", ""))
    monkeypatch.setattr(processor.shadow_action_engine, "compute", lambda **kwargs: [])
    monkeypatch.setattr(processor, "_check_crm_available", lambda: True)
    monkeypatch.setattr(processor, "detect_commitments", lambda *_args, **_kwargs: [
        Commitment(
            commitment_text="Отправлю файлы",
            deadline_iso="2024-07-12",
            status="pending",
            source="heuristic",
            confidence=0.9,
        )
    ])
    monkeypatch.setattr(
        processor,
        "evaluate_commitment_updates",
        lambda *_args, **_kwargs: [
            CommitmentStatusUpdate(
                commitment_id=10,
                commitment_text="Отправлю файлы",
                deadline_iso="2024-07-12",
                old_status="pending",
                new_status="fulfilled",
                reason="confirmation_text",
            ),
            CommitmentStatusUpdate(
                commitment_id=11,
                commitment_text="Согласую",
                deadline_iso="2024-07-01",
                old_status="pending",
                new_status="expired",
                reason="deadline_passed",
            ),
        ],
    )
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
            upsert_entity_signal=lambda **kwargs: None,
        ),
    )
    monkeypatch.setattr(
        processor.context_store,
        "resolve_sender_entity",
        lambda **kwargs: EntityResolution(
            entity_id="entity-1",
            entity_type="person",
            confidence=1.0,
        ),
    )
    monkeypatch.setattr(processor.context_store, "resolve_entity_relationships", lambda **kwargs: None)
    monkeypatch.setattr(processor.context_store, "record_interaction_event", lambda **kwargs: (None, None))
    monkeypatch.setattr(processor.context_store, "recompute_email_frequency", lambda **kwargs: (0.0, 0))
    monkeypatch.setattr(
        processor.trust_score_calculator,
        "compute",
        lambda **kwargs: TrustScoreResult(
            snapshot=TrustSnapshot(
                entity_id="entity-1",
                score=0.8,
                reason=None,
                sample_size=5,
            ),
            components=TrustScoreComponents(
                commitment_reliability=1.0,
                response_consistency=0.9,
                trend=0.5,
            ),
            data_window_days=60,
        ),
    )
    monkeypatch.setattr(
        processor.relationship_health_calculator,
        "compute",
        lambda **kwargs: HealthSnapshot(
            entity_id="entity-1",
            health_score=80.0,
            components_breakdown={"trust_score": 0.8},
            data_window_days=90,
            reason=None,
        ),
    )
    monkeypatch.setattr(processor, "trust_snapshot_writer", SimpleNamespace(write=lambda *args, **kwargs: None))
    monkeypatch.setattr(
        processor,
        "relationship_health_snapshot_writer",
        SimpleNamespace(write=lambda *args, **kwargs: None),
    )
    monkeypatch.setattr(processor, "send_preview_to_telegram", lambda **kwargs: None)

    sent: list[object] = []

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=101,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 7, 10, 12, 0),
        body_text="Text",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent
    event_types = _load_event_types(emitter.path)
    expected = {
        "email_received",
        "commitment_created",
        "commitment_fulfilled",
        "commitment_expired",
        "trust_score_updated",
        "relationship_health_updated",
        "telegram_payload_validated",
    }
    assert expected.issubset(set(event_types))


def test_event_emit_errors_do_not_break_pipeline(monkeypatch, tmp_path) -> None:
    emitter = EventEmitter(tmp_path / "events.sqlite")
    monkeypatch.setattr(processor, "event_emitter", emitter)
    monkeypatch.setattr(processor, "feature_flags", _feature_flags())
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _stub_llm_result())
    monkeypatch.setattr(processor.shadow_priority_engine, "compute", lambda *args, **kwargs: ("🔵", ""))
    monkeypatch.setattr(processor.shadow_action_engine, "compute", lambda **kwargs: [])
    monkeypatch.setattr(processor, "_check_crm_available", lambda: True)
    monkeypatch.setattr(processor, "detect_commitments", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
            upsert_entity_signal=lambda **kwargs: None,
        ),
    )
    monkeypatch.setattr(processor.context_store, "resolve_sender_entity", lambda **kwargs: None)
    monkeypatch.setattr(processor.context_store, "record_interaction_event", lambda **kwargs: (None, None))
    monkeypatch.setattr(processor.context_store, "recompute_email_frequency", lambda **kwargs: (0.0, 0))
    monkeypatch.setattr(processor, "trust_snapshot_writer", SimpleNamespace(write=lambda *args, **kwargs: None))
    monkeypatch.setattr(
        processor,
        "relationship_health_snapshot_writer",
        SimpleNamespace(write=lambda *args, **kwargs: None),
    )
    monkeypatch.setattr(processor, "send_preview_to_telegram", lambda **kwargs: None)

    def _broken_connect(*_args, **_kwargs):
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(
        "mailbot_v26.observability.event_emitter.sqlite3.connect",
        _broken_connect,
    )

    sent: list[dict[str, object]] = []

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent.append(payload)
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=102,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 7, 10, 12, 0),
        body_text="Text",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert sent
