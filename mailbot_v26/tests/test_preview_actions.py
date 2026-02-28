from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor


def _llm_result() -> SimpleNamespace:
    return SimpleNamespace(
        priority="🔴",
        action_line="Оплатить счет",
        body_summary="Body summary",
        attachment_summaries=[],
    )


def _setup(monkeypatch, *, enabled: bool, corrections: int) -> tuple[dict[str, object], dict[str, int]]:
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
    monkeypatch.setattr(
        processor,
        "feature_flags",
        SimpleNamespace(
            ENABLE_AUTO_PRIORITY=False,
            AUTO_PRIORITY_CONFIDENCE_THRESHOLD=0.6,
            ENABLE_AUTO_ACTIONS=True,
            AUTO_ACTION_CONFIDENCE_THRESHOLD=0.75,
            ENABLE_SHADOW_PERSISTENCE=False,
            ENABLE_PREVIEW_ACTIONS=enabled,
            ENABLE_PREMIUM_CLARITY_V1=False,
            ENABLE_ANOMALY_ALERTS=False,
        ),
    )
    monkeypatch.setattr(
        processor,
        "knowledge_db",
        SimpleNamespace(save_email=lambda **kwargs: None, save_preview_action=lambda **kwargs: None),
    )
    payload_store: dict[str, object] = {}
    monkeypatch.setattr(processor, "enqueue_tg", lambda *, email_id, payload: payload_store.setdefault("payload", payload))

    calls = {"count": 0}

    class _Analytics:
        def count_all_time_corrections(self, *, account_emails: list[str]) -> int:
            calls["count"] += 1
            return corrections

    monkeypatch.setattr(processor, "analytics", _Analytics())
    processor._preview_corrections_cache.clear()
    return payload_store, calls


def _process(monkeypatch, *, message_id: int = 1) -> str:
    processor.process_message(
        account_email="account@example.com",
        message_id=message_id,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="Body",
        attachments=[],
        telegram_chat_id="chat",
    )
    return "ok"


def test_preview_hidden_when_corrections_zero(monkeypatch) -> None:
    payload, calls = _setup(monkeypatch, enabled=True, corrections=0)
    _process(monkeypatch, message_id=10)
    assert "💡" not in payload["payload"].html_text


def test_preview_hidden_when_corrections_below_threshold(monkeypatch) -> None:
    payload, _ = _setup(monkeypatch, enabled=True, corrections=9)
    _process(monkeypatch, message_id=11)
    assert "💡" not in payload["payload"].html_text


def test_preview_shown_inline_when_threshold_met(monkeypatch) -> None:
    payload, calls = _setup(monkeypatch, enabled=True, corrections=10)
    _process(monkeypatch, message_id=12)
    assert "💡 Оплатить счет" in payload["payload"].html_text


def test_preview_disabled_flag(monkeypatch) -> None:
    payload, _ = _setup(monkeypatch, enabled=False, corrections=999)
    _process(monkeypatch, message_id=13)
    assert "💡" not in payload["payload"].html_text


def test_preview_corrections_cache_uses_single_query(monkeypatch) -> None:
    _, calls = _setup(monkeypatch, enabled=True, corrections=10)
    _process(monkeypatch, message_id=14)
    _process(monkeypatch, message_id=15)
    assert calls["count"] == 1
