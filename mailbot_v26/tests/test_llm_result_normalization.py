from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.llm.runtime_flags import RuntimeFlags
from mailbot_v26.pipeline import processor
from mailbot_v26.worker.telegram_sender import DeliveryResult


def _setup(monkeypatch) -> dict[str, object]:
    sent: dict[str, object] = {}
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
        SimpleNamespace(
            get_flags=lambda **kwargs: (RuntimeFlags(enable_auto_priority=False), False)
        ),
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

    def _enqueue_tg(*, email_id: int, payload):
        sent["payload"] = payload
        return DeliveryResult(delivered=True, retryable=False)

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    monkeypatch.setattr(processor, "send_system_notice", lambda **kwargs: None)
    return sent


def test_direct_llm_dict_result_is_normalized_and_used(monkeypatch) -> None:
    sent = _setup(monkeypatch)
    monkeypatch.setattr(
        processor,
        "run_llm_stage",
        lambda **kwargs: {
            "summary": "Счёт на оплату",
            "action_line": "Оплатить сегодня",
            "priority": "🔴",
            "attachment_summaries": [],
        },
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=501,
        from_email="sender@example.com",
        subject="Invoice",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Оплатить до конца дня",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = sent["payload"]
    assert payload.priority == "🔴"
    assert payload.metadata["body_summary"] == "Счёт на оплату"


def test_direct_llm_dict_without_priority_keeps_heuristic_priority(monkeypatch) -> None:
    sent = _setup(monkeypatch)
    monkeypatch.setattr(
        processor,
        "run_llm_stage",
        lambda **kwargs: {
            "summary": "Просто обновление",
            "action_line": "Проверить",
            "attachment_summaries": [],
        },
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=502,
        from_email="sender@example.com",
        subject="Status update",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="FYI",
        attachments=[],
        telegram_chat_id="chat",
    )

    payload = sent["payload"]
    assert payload.priority == "🔵"
    assert payload.metadata["body_summary"] == "Просто обновление"
