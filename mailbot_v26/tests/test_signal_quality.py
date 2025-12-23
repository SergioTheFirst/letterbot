from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.signal_quality import (
    MIN_ENTROPY,
    MIN_LENGTH,
    MIN_PRINTABLE_RATIO,
    evaluate_signal_quality,
)


def test_signal_quality_low_entropy() -> None:
    text = "a" * (MIN_LENGTH + 5)
    quality = evaluate_signal_quality(text)
    assert quality.length >= MIN_LENGTH
    assert quality.entropy < MIN_ENTROPY
    assert quality.is_usable is False
    assert quality.reason == "entropy_below_threshold"


def test_signal_quality_normal_text() -> None:
    text = ("The quick brown fox jumps over the lazy dog 1234567890! ") * 2
    quality = evaluate_signal_quality(text)
    assert quality.entropy > 3.0
    assert quality.printable_ratio >= MIN_PRINTABLE_RATIO
    assert quality.is_usable is True
    assert quality.reason == "ok"


def test_signal_quality_printable_ratio_edge() -> None:
    noisy = "".join(chr(i) for i in range(1, 33)) * 3
    quality = evaluate_signal_quality(noisy)
    assert quality.length >= MIN_LENGTH
    assert quality.entropy > MIN_ENTROPY
    assert quality.printable_ratio < MIN_PRINTABLE_RATIO
    assert quality.is_usable is False
    assert quality.reason == "printable_ratio_below_threshold"


def _configure_minimal_processor(monkeypatch, llm_result) -> dict[str, str]:
    captured: dict[str, str] = {}

    def _fake_run_llm_stage(**kwargs):
        captured["body_text"] = kwargs.get("body_text", "")
        return llm_result

    monkeypatch.setattr(processor, "run_llm_stage", _fake_run_llm_stage)
    monkeypatch.setattr(
        processor, "knowledge_db", SimpleNamespace(save_email=lambda **kwargs: None)
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
    monkeypatch.setattr(processor, "enqueue_tg", lambda **kwargs: None)
    return captured


def test_signal_fallback_used_for_low_entropy(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Ответить",
        body_summary="Summary",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    captured = _configure_minimal_processor(monkeypatch, llm_result)

    processor.process_message(
        account_email="account@example.com",
        message_id=100,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text="a" * (MIN_LENGTH + 10),
        attachments=[],
        telegram_chat_id="chat",
    )

    assert "Тело письма недоступно" in captured["body_text"]
    assert "Тема: Subject" in captured["body_text"]
    assert "От: sender@example.com" in captured["body_text"]


def test_signal_fallback_not_used_for_normal_text(monkeypatch) -> None:
    llm_result = SimpleNamespace(
        priority="🔵",
        action_line="Ответить",
        body_summary="Summary",
        attachment_summaries=[],
        llm_provider="gigachat",
    )
    captured = _configure_minimal_processor(monkeypatch, llm_result)
    body_text = "The quick brown fox jumps over the lazy dog 1234567890!"

    processor.process_message(
        account_email="account@example.com",
        message_id=101,
        from_email="sender@example.com",
        subject="Subject",
        received_at=datetime(2024, 1, 1, 12, 0),
        body_text=body_text,
        attachments=[],
        telegram_chat_id="chat",
    )

    assert captured["body_text"] == body_text
