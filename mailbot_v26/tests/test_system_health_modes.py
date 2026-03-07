from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult
from mailbot_v26.system_health import OperationalMode, system_health


def _setup_pipeline(monkeypatch, *, llm_result) -> None:
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: llm_result)
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, path=":memory:"),
    )
    monkeypatch.setattr(
        processor,
        "decision_trace_writer",
        SimpleNamespace(write=lambda **kwargs: None),
    )
    monkeypatch.setattr(
        processor,
        "context_store",
        SimpleNamespace(
            resolve_sender_entity=lambda **kwargs: None,
            record_interaction_event=lambda **kwargs: (None, None),
            recompute_email_frequency=lambda **kwargs: (0.0, 0),
        ),
    )
    monkeypatch.setattr(
        processor,
        "shadow_priority_engine",
        SimpleNamespace(compute=lambda **kwargs: (llm_result.priority, None)),
    )
    monkeypatch.setattr(
        processor,
        "shadow_action_engine",
        SimpleNamespace(compute=lambda **kwargs: []),
    )
    monkeypatch.setattr(
        processor,
        "runtime_flag_store",
        SimpleNamespace(get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=False), False)),
    )
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            ENABLE_AUTO_ACTIONS=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.8,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=False,
        ),
    )
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **kwargs: DeliveryResult(delivered=True, retryable=False),
    )
    monkeypatch.setattr(processor, "send_system_notice", lambda **kwargs: None)


def test_llm_unavailable_sets_degraded_mode(monkeypatch) -> None:
    system_health.reset()
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: None)
    monkeypatch.setattr(processor, "send_system_notice", lambda **kwargs: None)

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert system_health.mode == OperationalMode.DEGRADED_NO_LLM


def test_crm_unavailable_sets_emergency_read_only(monkeypatch) -> None:
    system_health.reset()
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Ответить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="cloudflare",
    )
    _setup_pipeline(monkeypatch, llm_result=llm_result)

    def _raise_save(**kwargs):
        raise RuntimeError("CRM down")

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=_raise_save, path=":memory:"),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=2,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert system_health.mode == OperationalMode.EMERGENCY_READ_ONLY


def test_recovery_returns_full_mode(monkeypatch) -> None:
    system_health.reset()
    system_health.update_component("LLM", False, reason="down")

    llm_result = SimpleNamespace(
        priority="🟡",
        action_line="Проверить",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    _setup_pipeline(monkeypatch, llm_result=llm_result)

    processor.process_message(
        account_email="account@example.com",
        message_id=3,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert system_health.mode == OperationalMode.FULL
