from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26 import config_loader
from mailbot_v26.events.contract import EventType
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


class _Collector:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def _write_accounts(tmp_path, content: str) -> None:
    (tmp_path / "accounts.ini").write_text(content, encoding="utf-8")


def test_delivery_policy_event_payload_and_render_record(
    monkeypatch, tmp_path
) -> None:
    _write_accounts(
        tmp_path,
        """[primary]
login = account@example.com
password = secret
telegram_chat_id = chat

[alt]
login = alt@example.com
password = secret
telegram_chat_id = chat
""",
    )
    monkeypatch.setattr(config_loader, "CONFIG_DIR", tmp_path)
    config_loader._load_account_scopes.cache_clear()
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Срочно ответить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_PREMIUM_CLARITY_V1=False,
            ENABLE_CIRCADIAN_DELIVERY=True,
            ENABLE_FLOW_PROTECTION=False,
            ENABLE_ATTENTION_DEBT=False,
            ENABLE_PRIORITY_V2=False,
        ),
    )
    collector = _Collector()
    monkeypatch.setattr(processor, "contract_event_emitter", collector)

    def _enqueue_tg(*, email_id: int, payload) -> None:
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=99,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    event_types = [event.event_type for event in collector.events]
    assert EventType.DELIVERY_POLICY_APPLIED in event_types
    assert EventType.TG_RENDER_RECORDED in event_types

    decision_event = next(
        event for event in collector.events if event.event_type == EventType.DELIVERY_POLICY_APPLIED
    )
    payload = decision_event.payload
    assert "subject" not in payload
    assert "sender" not in payload
    assert payload["priority"] == "🔴"
    assert "sources" in payload
    assert payload["chat_scope"] == "tg:chat"
    assert payload["account_emails"] == ["account@example.com", "alt@example.com"]

    render_event = next(
        event for event in collector.events if event.event_type == EventType.TG_RENDER_RECORDED
    )
    render_payload = render_event.payload
    assert set(render_payload.keys()) == {
        "shown_fact_types",
        "fact_sources",
        "extraction_failed",
        "confidence_bucket",
        "attachments_count",
        "suppressed_numeric_facts",
        "has_attachment_fact_provenance",
    }
    assert render_payload["confidence_bucket"] in {"hi", "med", "low", "na"}
    assert isinstance(render_payload["attachments_count"], int)
    assert render_payload["shown_fact_types"] == []
    assert render_payload["fact_sources"] == []
    assert isinstance(render_payload["suppressed_numeric_facts"], bool)
    assert isinstance(render_payload["has_attachment_fact_provenance"], bool)
    assert render_payload["suppressed_numeric_facts"] is True
    assert render_payload["has_attachment_fact_provenance"] is False


def test_delivery_policy_event_payload_without_scope(
    monkeypatch, tmp_path
) -> None:
    _write_accounts(
        tmp_path,
        """[primary]
login = account@example.com
password = secret
""",
    )
    monkeypatch.setattr(config_loader, "CONFIG_DIR", tmp_path)
    config_loader._load_account_scopes.cache_clear()
    llm_result = SimpleNamespace(
        priority="🔴",
        action_line="Срочно ответить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None))
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
            ENABLE_PREMIUM_CLARITY_V1=False,
            ENABLE_CIRCADIAN_DELIVERY=True,
            ENABLE_FLOW_PROTECTION=False,
            ENABLE_ATTENTION_DEBT=False,
            ENABLE_PRIORITY_V2=False,
        ),
    )
    collector = _Collector()
    monkeypatch.setattr(processor, "contract_event_emitter", collector)

    def _enqueue_tg(*, email_id: int, payload) -> None:
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)

    processor.process_message(
        account_email="account@example.com",
        message_id=99,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    decision_event = next(
        event for event in collector.events if event.event_type == EventType.DELIVERY_POLICY_APPLIED
    )
    payload = decision_event.payload
    assert "chat_scope" not in payload
    assert "account_emails" not in payload
