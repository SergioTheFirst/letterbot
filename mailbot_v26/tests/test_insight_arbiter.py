from __future__ import annotations

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.pipeline.insight_arbiter import (
    InsightArbiterInput,
    apply_insight_arbiter,
)


def test_arbiter_replaces_low_signal_summary_with_attachment_fallback() -> None:
    result = apply_insight_arbiter(
        InsightArbiterInput(
            llm_summary="проверить письмо",
            extracted_text_len=0,
            attachment_details=[
                {"kind": "PDF", "chars": 1200},
                {"kind": "XLS", "chars": 0},
            ],
            commitments=[],
        )
    )

    assert result.replaced is True
    assert result.reason == "summary_low_signal"
    assert result.summary == (
        "Автоматическая сводка слишком общая. Письмо содержит 2 вложений "
        "(PDF, XLS); текст во вложениях: 1200 символов, текст письма не извлечён."
    )


def test_arbiter_preserves_summary_when_commitments_present() -> None:
    commitment = Commitment(
        commitment_text="Отправлю отчет завтра",
        deadline_iso="2024-01-02",
        status="pending",
        source="heuristic",
        confidence=0.9,
    )
    result = apply_insight_arbiter(
        InsightArbiterInput(
            llm_summary="проверить письмо",
            extracted_text_len=0,
            attachment_details=[],
            commitments=[commitment],
        )
    )

    assert result.replaced is False
    assert result.reason == "commitments_present"
    assert result.summary == "проверить письмо"


def test_arbiter_reports_extraction_failure() -> None:
    result = apply_insight_arbiter(
        InsightArbiterInput(
            llm_summary="проверить письмо",
            extracted_text_len=0,
            attachment_details=[],
            commitments=[],
        )
    )

    assert result.replaced is True
    assert result.reason == "extraction_failed"
    assert (
        result.summary
        == "Не удалось извлечь текст письма или вложений; требуется ручной просмотр."
    )
