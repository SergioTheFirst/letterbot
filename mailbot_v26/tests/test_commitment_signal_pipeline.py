import json
import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.storage.context_layer import EntityResolution


def test_commitment_signal_crm_failure_does_not_break_pipeline(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
        ENABLE_COMMITMENT_TRACKER=True,
    )
    monkeypatch.setattr(processor, "feature_flags", flags)

    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: SimpleNamespace(
        priority="🔴",
        action_line="Оплатить счет",
        body_summary="Body summary",
        attachment_summaries=[],
    ))
    monkeypatch.setattr(
        processor.shadow_priority_engine,
        "compute",
        lambda llm_priority, from_email: ("🔴", "shadow reason"),
    )
    monkeypatch.setattr(
        processor.shadow_action_engine,
        "compute",
        lambda account_email, from_email: [("Оплатить счет", "shadow action reason")],
    )
    monkeypatch.setattr(
        processor.auto_action_engine,
        "propose",
        lambda **kwargs: {
            "type": "PAYMENT",
            "text": "Оплатить счет",
            "source": "shadow",
            "confidence": 0.9,
        },
    )
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)
    monkeypatch.setattr(processor, "send_preview_to_telegram", lambda **kwargs: None)

    class _Analytics:
        def commitment_stats_by_sender(self, *, from_email: str, days: int = 30) -> dict[str, int]:
            return {
                "total_commitments": 2,
                "fulfilled_count": 1,
                "expired_count": 1,
                "unknown_count": 0,
            }

        def sender_stats(self, limit=None):  # pragma: no cover - defensive
            return []

        def priority_escalations(self, limit=None):  # pragma: no cover - defensive
            return []

    monkeypatch.setattr(processor, "analytics", _Analytics())
    monkeypatch.setattr(
        processor.context_store,
        "resolve_sender_entity",
        lambda **kwargs: EntityResolution(
            entity_id="entity-1",
            entity_type="person",
            confidence=1.0,
        ),
    )
    monkeypatch.setattr(
        processor.context_store,
        "record_interaction_event",
        lambda **kwargs: (None, None),
    )
    monkeypatch.setattr(
        processor.context_store,
        "recompute_email_frequency",
        lambda **kwargs: (0.0, 0),
    )

    def _raise_upsert(**kwargs) -> None:
        raise RuntimeError("crm fail")

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
            upsert_entity_signal=_raise_upsert,
        ),
    )

    caplog.set_level(logging.ERROR)

    processor.process_message(
        account_email="account@example.com",
        message_id=701,
        from_email="client@company.com",
        subject="Subject",
        received_at=datetime(2024, 7, 10, 12, 0),
        body_text="Пришлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert any(
        json.loads(record.message).get("event") == "entity_signal_compute_failed"
        for record in caplog.records
        if record.message.startswith("{")
    )
