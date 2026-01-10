import json
import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.storage.context_layer import EntityResolution
from mailbot_v26.system_health import OperationalMode

_DEFAULT_PROPOSAL = object()


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Оплатить счет",
        body_summary="Body summary",
        attachment_summaries=[{"filename": "file.txt", "summary": "summary"}],
    )


def _common_monkeypatches(monkeypatch, flags, proposed_action=_DEFAULT_PROPOSAL) -> dict[str, object]:
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
        lambda **kwargs: (
            proposed_action
            if proposed_action is not _DEFAULT_PROPOSAL
            else {
                "type": "PAYMENT",
                "text": "Оплатить счет",
                "source": "shadow",
                "confidence": 0.9,
            }
        ),
    )
    monkeypatch.setattr(processor, "feature_flags", flags)

    payload_store: dict[str, object] = {}

    def _enqueue_tg(*, email_id: int, payload) -> None:
        payload_store["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    return payload_store


def _preview_capture(monkeypatch) -> dict[str, object]:
    preview_payload: dict[str, object] = {}
    monkeypatch.setattr(
        processor,
        "send_preview_to_telegram",
        lambda **kwargs: preview_payload.update(kwargs),
    )
    return preview_payload


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
    preview_payload = _preview_capture(monkeypatch)

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
    assert not any(
        json.loads(record.message).get("event") == "preview_shown"
        for record in caplog.records
        if record.message.startswith("{")
    )
    assert preview_payload == {}
    assert payload["payload"].metadata.get("chat_id") == "chat"


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
    preview_payload = _preview_capture(monkeypatch)

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
    assert any(
        json.loads(record.message).get("event") == "preview_shown"
        for record in caplog.records
        if record.message.startswith("{")
    )
    assert payload["payload"].metadata.get("chat_id") == "chat"
    assert preview_payload.get("chat_id") == "chat"
    preview_text = str(preview_payload.get("preview_text") or "")
    assert preview_text.startswith("AI-превью")
    assert "Предлагаемое действие:" in preview_text
    assert "• Оплатить счет" in preview_text
    assert "Причина:" in preview_text
    assert "ПОЧЕМУ ТАК:" in preview_text
    assert "Уверенность: 0.90" in preview_text
    assert "[Принять] [Отклонить]" in preview_text
    assert "[Сделать Высокий] [Сделать Средний] [Сделать Низкий]" in preview_text
    for forbidden in ("<", ">", "*", "_", "</"):
        assert forbidden not in preview_text


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
    _preview_capture(monkeypatch)

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

    def _enqueue_tg(*, email_id: int, payload) -> None:
        preview_payload["payload"] = payload

    monkeypatch.setattr(processor, "enqueue_tg", _enqueue_tg)
    _preview_capture(monkeypatch)

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

    baseline = baseline_payload["payload"]
    previewed = preview_payload["payload"]
    assert (baseline.html_text, baseline.priority, baseline.metadata) == (
        previewed.html_text,
        previewed.priority,
        previewed.metadata,
    )


def test_preview_enabled_no_proposals(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
    )
    _common_monkeypatches(monkeypatch, flags, proposed_action=None)
    preview_payload = _preview_capture(monkeypatch)

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=lambda **kwargs: None),
    )
    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=301,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 3, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert preview_payload == {}
    assert any(
        json.loads(record.message).get("event") == "preview_actions_skipped"
        and json.loads(record.message).get("reason") == "no_proposals"
        for record in caplog.records
        if record.message.startswith("{")
    )


def test_preview_skipped_when_llm_degraded(monkeypatch, caplog) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
    )
    _common_monkeypatches(monkeypatch, flags)
    preview_payload = _preview_capture(monkeypatch)

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=lambda **kwargs: None),
    )
    degraded_health = SimpleNamespace(
        mode=OperationalMode.DEGRADED_NO_LLM,
        update_component=lambda *args, **kwargs: None,
        system_notice=lambda change: "",
    )
    monkeypatch.setattr(processor, "system_health", degraded_health)
    caplog.set_level(logging.INFO)

    processor.process_message(
        account_email="account@example.com",
        message_id=401,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 4, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )

    assert preview_payload == {}
    assert any(
        json.loads(record.message).get("event") == "preview_actions_skipped"
        and json.loads(record.message).get("reason") == "system_degraded_no_llm"
        for record in caplog.records
        if record.message.startswith("{")
    )


def test_commitments_preview_flag_off(monkeypatch) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
        ENABLE_COMMITMENT_TRACKER=False,
    )
    _common_monkeypatches(monkeypatch, flags)
    preview_payload = _preview_capture(monkeypatch)

    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
        ),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=501,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 5, 1, 12, 0),
        body_text="Вышлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    preview_text = str(preview_payload.get("preview_text") or "")
    assert "Обязательства" not in preview_text


def test_commitments_preview_block_and_payload_unchanged(monkeypatch) -> None:
    flags_off = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
        ENABLE_COMMITMENT_TRACKER=False,
    )
    flags_on = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
        ENABLE_COMMITMENT_TRACKER=True,
    )

    baseline_payload = _common_monkeypatches(monkeypatch, flags_off)
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
        ),
    )
    _preview_capture(monkeypatch)

    processor.process_message(
        account_email="account@example.com",
        message_id=502,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 6, 1, 12, 0),
        body_text="Пришлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    commitment_payload = _common_monkeypatches(monkeypatch, flags_on)
    preview_payload = _preview_capture(monkeypatch)
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 2,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
        ),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=503,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 6, 2, 12, 0),
        body_text="Пришлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    preview_text = str(preview_payload.get("preview_text") or "")
    assert "Обязательства" in preview_text
    assert "• \"Пришлю отчет до 25.12.2025\" — ожидается" in preview_text
    baseline = baseline_payload["payload"]
    commitment = commitment_payload["payload"]
    assert (baseline.html_text, baseline.priority, baseline.metadata) == (
        commitment.html_text,
        commitment.priority,
        commitment.metadata,
    )


def test_commitment_signal_preview_block(monkeypatch) -> None:
    flags = SimpleNamespace(
        ENABLE_AUTO_PRIORITY=False,
        AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
        ENABLE_AUTO_ACTIONS=True,
        AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
        ENABLE_SHADOW_PERSISTENCE=False,
        ENABLE_PREVIEW_ACTIONS=True,
        ENABLE_COMMITMENT_TRACKER=True,
    )
    _common_monkeypatches(monkeypatch, flags)
    preview_payload = _preview_capture(monkeypatch)

    class _Analytics:
        def commitment_stats_by_sender(self, *, from_email: str, days: int = 30) -> dict[str, int]:
            return {
                "total_commitments": 4,
                "fulfilled_count": 3,
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
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(
            save_email=lambda **kwargs: 1,
            save_preview_action=lambda **kwargs: None,
            save_commitments=lambda **kwargs: True,
            fetch_pending_commitments_by_sender=lambda **kwargs: [],
            update_commitment_statuses=lambda **kwargs: True,
            upsert_entity_signal=lambda **kwargs: None,
        ),
    )

    processor.process_message(
        account_email="account@example.com",
        message_id=601,
        from_email="client@company.com",
        subject="Subject",
        received_at=datetime(2024, 7, 1, 12, 0),
        body_text="Пришлю отчет до 25.12.2025.",
        attachments=[],
        telegram_chat_id="chat",
    )

    preview_text = str(preview_payload.get("preview_text") or "")
    assert "Контекст отношений:" in preview_text
    assert "Контрагент: client@company.com" in preview_text
    assert "Надёжность обязательств: 🟡 Нестабилен 75/100" in preview_text
    assert "(выполнено: 3, просрочено: 1 за 30 дней)" in preview_text
