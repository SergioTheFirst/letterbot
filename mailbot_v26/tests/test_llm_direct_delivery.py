from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.config.llm_queue import LLMQueueConfig
from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup_runtime(monkeypatch) -> None:
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
        SimpleNamespace(compute=lambda **kwargs: ("🔵", None)),
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
            ENABLE_PRIORITY_V2=False,
        ),
    )
    monkeypatch.setattr(processor, "send_system_notice", lambda **kwargs: None)


def test_direct_llm_preferred_over_queue_heuristic(monkeypatch) -> None:
    _setup_runtime(monkeypatch)
    calls = {"direct": 0}

    def _direct_llm(**kwargs):
        calls["direct"] += 1
        return SimpleNamespace(
            priority="🔵",
            action_line="Ознакомиться",
            body_summary="Direct summary",
            attachment_summaries=[],
            llm_provider="cloudflare",
        )

    monkeypatch.setattr(processor, "run_llm_stage", _direct_llm)
    monkeypatch.setattr(
        processor,
        "get_llm_queue_config",
        lambda: LLMQueueConfig(llm_request_queue_enabled=True, max_concurrent_llm_calls=1),
    )
    monkeypatch.setattr(
        processor,
        "enqueue_tg",
        lambda **kwargs: DeliveryResult(delivered=True, retryable=False),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=10,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert calls["direct"] == 1


def test_heuristic_fallback_used_when_direct_llm_fails(monkeypatch) -> None:
    _setup_runtime(monkeypatch)
    captured = {"text": ""}

    def _failing_llm(**kwargs):
        raise TimeoutError("timeout")

    def _enqueue(*, email_id: int, payload):
        captured["text"] = payload.html_text
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "run_llm_stage", _failing_llm)
    monkeypatch.setattr(
        processor,
        "get_llm_queue_config",
        lambda: LLMQueueConfig(llm_request_queue_enabled=False, max_concurrent_llm_calls=1),
    )
    monkeypatch.setattr(processor, "enqueue_tg", _enqueue)

    processor.process_message(
        account_email="account@example.com",
        message_id=11,
        from_email="sender@example.com",
        subject="Timeout subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Fallback body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert "Timeout subject" in captured["text"]
