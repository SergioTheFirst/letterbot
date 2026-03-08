from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from mailbot_v26.behavior.threading import compute_thread_key
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup_processor(monkeypatch, captured: dict) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Проверить письмо",
        body_summary="Краткое описание письма.",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    monkeypatch.setattr(processor, "run_llm_stage", lambda **_kwargs: llm_result)

    def _save_email(**kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(
        processor, "knowledge_db", SimpleNamespace(save_email=_save_email)
    )
    monkeypatch.setattr(processor, "_check_crm_available", lambda: True)
    monkeypatch.setattr(
        processor,
        "decision_trace_writer",
        SimpleNamespace(write=lambda **_kwargs: None),
    )
    monkeypatch.setattr(
        processor, "event_emitter", SimpleNamespace(emit=lambda **_kwargs: None)
    )
    monkeypatch.setattr(
        processor, "contract_event_emitter", SimpleNamespace(emit=lambda _event: True)
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
        processor.context_store, "resolve_sender_entity", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        processor.context_store,
        "record_interaction_event",
        lambda **_kwargs: (None, None),
    )
    monkeypatch.setattr(
        processor.context_store,
        "recompute_email_frequency",
        lambda **_kwargs: (0.0, 0),
    )
    monkeypatch.setattr(
        processor,
        "evaluate_signal_quality",
        lambda *_args, **_kwargs: SimpleNamespace(
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
        lambda *_args, **_kwargs: SimpleNamespace(deferred=False, reason="test"),
    )


def test_thread_key_persisted_on_premium_path(monkeypatch) -> None:
    captured: dict = {}
    _setup_processor(monkeypatch, captured)

    processor.process_message(
        account_email="account@example.com",
        message_id=1,
        from_email="sender@example.com",
        from_name=None,
        subject="Re: Hello",
        received_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        body_text="Hello",
        attachments=[],
        telegram_chat_id="chat-id",
        rfc_message_id="<msg@example.com>",
        in_reply_to="<root@example.com>",
        references=None,
    )

    expected = compute_thread_key(
        account_email="account@example.com",
        rfc_message_id="<msg@example.com>",
        in_reply_to="<root@example.com>",
        references=None,
        subject="Re: Hello",
        from_email="sender@example.com",
    )
    assert captured["thread_key"] == expected
    assert captured["rfc_message_id"] == "<msg@example.com>"
    assert captured["in_reply_to"] == "<root@example.com>"
