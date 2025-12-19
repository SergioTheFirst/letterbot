import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Оплатить счет",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def _common_monkeypatches(monkeypatch, flags) -> dict[str, object]:
    monkeypatch.setattr(processor, "run_llm_stage", lambda **kwargs: _llm_result())
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
    monkeypatch.setattr(processor, "feature_flags", flags)

    payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "send_to_telegram", lambda **kwargs: payload.update(kwargs))
    return payload


def test_preview_disabled_no_preview_generated(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
    )
    payload = _common_monkeypatches(monkeypatch, flags)

    preview_called = False

    def _preview_called(**kwargs) -> None:  # pragma: no cover - defensive
        nonlocal preview_called
        preview_called = True

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=_preview_called),
    )

    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=101,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert preview_called is False
    assert not any("[PREVIEW]" in record.message for record in caplog.records)
    assert "chat_id" in payload


def test_preview_enabled_preview_generated(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
    )
    payload = _common_monkeypatches(monkeypatch, flags)

    stored: dict[str, object] = {}

    def _save_preview_action(**kwargs) -> None:
        stored.update(kwargs)

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=_save_preview_action),
    )

    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=102,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert stored.get("email_id") == 102
    assert stored.get("proposed_action")
    assert any("[PREVIEW]" in record.message for record in caplog.records)
    assert "chat_id" in payload


def test_preview_does_not_change_telegram_payload(monkeypatch) -> None:
    flags_off = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=False,
    )
    flags_on = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
    )

    baseline_payload = _common_monkeypatches(monkeypatch, flags_off)
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=lambda **kwargs: None),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=201,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 2, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    preview_payload: dict[str, object] = {}
    monkeypatch.setattr(processor, "feature_flags", flags_on)
    monkeypatch.setattr(
        processor,
        "send_to_telegram",
        lambda **kwargs: preview_payload.update(kwargs),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=202,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 2, 2, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert baseline_payload == preview_payload
