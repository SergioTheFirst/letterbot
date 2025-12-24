from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.insights.relationship_health import HealthSnapshot
from mailbot_v26.pipeline import processor
from mailbot_v26.telegram_utils import telegram_safe
from mailbot_v26.storage.context_layer import EntityResolution
from mailbot_v26.worker.telegram_sender import TelegramSendResult


def _common_flags() -> SimpleNamespace:
    return SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=False,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
        ENABLE_COMMITMENT_TRACKER=False,
    )


def _stub_llm_result():
    return SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary="Body summary",
        attachment_summaries=[],
    )


def test_relationship_health_does_not_change_telegram_payload(monkeypatch) -> None:
    monkeypatch.setattr(processor, "feature_flags", _common_flags())
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _stub_llm_result())
    monkeypatch.setattr(processor.shadow_priority_engine, "compute", lambda *args, **kwargs: ("🔵", ""))
    monkeypatch.setattr(processor.shadow_action_engine, "compute", lambda *args, **kwargs: [])
    monkeypatch.setattr(processor, "send_preview_to_telegram", lambda **kwargs: None)

    sent: list[object] = []

    def _enqueue_tg(*, email_id: int, payload) -> None:
        sent.append(payload)
        return TelegramSendResult(success=True)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    monkeypatch.setattr(
        processor.context_store,
        "resolve_sender_entity",
        lambda **kwargs: EntityResolution(
            entity_id="entity-1",
            entity_type="person",
            confidence=1.0,
        ),
    )
    monkeypatch.setattr(processor.context_store, "record_interaction_event", lambda **kwargs: (None, None))
    monkeypatch.setattr(processor.context_store, "recompute_email_frequency", lambda **kwargs: (0.0, 0))
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: 1))
    monkeypatch.setattr(
        processor,
        "trust_snapshot_writer",
        SimpleNamespace(write=lambda *args, **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "relationship_health_snapshot_writer",
        SimpleNamespace(write=lambda *args, **kwargs: None),
    )
    monkeypatch.setattr(
        processor.trust_score_calculator,
        "compute",
        lambda **kwargs: SimpleNamespace(
            snapshot=SimpleNamespace(score=0.8, sample_size=5),
            components=SimpleNamespace(
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
            health_score=75.0,
            components_breakdown={},
            data_window_days=90,
            reason=None,
        ),
    )
    monkeypatch.setattr(
        processor.relationship_anomaly_detector,
        "detect",
        lambda **kwargs: [],
    )

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

    base_text = processor._build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Проверить письмо",
        body_summary="Body summary",
        body_text="Text",
        attachment_summary="",
    )
    if "Text" not in base_text:
        base_text = f"{base_text}\n\n{processor._trim_telegram_body('Text')}"
    telegram_text = telegram_safe(base_text)

    assert len(sent) == 1
    payload = sent[0]
    assert payload.priority == "🔵"
    assert payload.html_text == telegram_text
    assert payload.metadata["chat_id"] == "chat"
    assert payload.metadata["account_email"] == "account@example.com"
    assert payload.metadata["action_line"] == "Проверить письмо"
    assert payload.metadata["body_summary"] == "Body summary"
    assert payload.metadata["attachment_summaries"] == []
