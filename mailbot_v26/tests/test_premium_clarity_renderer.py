from __future__ import annotations

from datetime import datetime
import re

from mailbot_v26.insights.digest import InsightDigest
from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.pipeline import processor


def _render(
    *,
    priority: str = "🔵",
    attachments: list[dict[str, object]] | None = None,
    attachment_summaries: list[dict[str, object]] | None = None,
    body_summary: str = "Краткий факт письма.",
    body_text: str = "Краткий факт письма.",
    extraction_failed: bool = False,
    confidence_percent: int = 80,
    confidence_available: bool = True,
    confidence_dots_mode: str = "auto",
    confidence_dots_threshold: int = 75,
    confidence_dots_scale: int = 10,
    insight_digest: InsightDigest | None = None,
    subject: str = "Тема письма",
    received_at: datetime | None = None,
    commitments: list[Commitment] | None = None,
    insights: list[processor.Insight] | None = None,
) -> str:
    return processor._build_premium_clarity_text(
        priority=priority,
        received_at=received_at or datetime(2026, 1, 1),
        from_email="sender@example.com",
        from_name="Sender",
        subject=subject,
        action_line="Ответить клиенту",
        body_summary=body_summary,
        body_text=body_text,
        attachments=attachments or [],
        attachment_summaries=attachment_summaries or [],
        insights=insights or [],
        insight_digest=insight_digest,
        commitments=commitments or [],
        attachments_count=len(attachments or []),
        extracted_text_len=0 if extraction_failed else 120,
        confidence_percent=confidence_percent,
        confidence_available=confidence_available,
        confidence_dots_mode=confidence_dots_mode,
        confidence_dots_threshold=confidence_dots_threshold,
        confidence_dots_scale=confidence_dots_scale,
        extraction_failed=extraction_failed,
    )


def _extract_dots(text: str) -> str:
    return "".join(char for char in text if char in {"●", "○"})


def test_premium_clarity_includes_required_lines() -> None:
    rendered = _render()
    lines = rendered.splitlines()
    assert lines[0].startswith("🔵 ")
    assert lines[1].startswith("От: ")
    assert lines[2].startswith("Тема: ")
    assert any(line.startswith("📎 Вложения (0):") for line in lines)
    assert any(line.startswith("💬 ") or line.startswith("⚡ ") or line.startswith("⏸️ ") for line in lines)


def test_premium_clarity_attachment_truncation() -> None:
    attachments = [
        {"filename": "one.pdf", "text": "text"},
        {"filename": "two.pdf", "text": "text"},
        {"filename": "three.pdf", "text": ""},
        {"filename": "four.pdf", "text": ""},
    ]
    rendered = _render(attachments=attachments)
    lines = rendered.splitlines()
    assert "📎 Вложения (4):" in lines
    assert any("... и ещё 1" in line for line in lines)
    assert "• one.pdf" in lines


def test_premium_clarity_reason_for_critical() -> None:
    rendered = _render(priority="🔴")
    assert any(line.startswith("Причина:") for line in rendered.splitlines())


def test_premium_clarity_spoiler_optional() -> None:
    rendered = _render()
    assert "<tg-spoiler>" not in rendered
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered_with_spoiler = _render(insight_digest=digest)
    assert "<tg-spoiler>" in rendered_with_spoiler


def test_premium_clarity_line_limit() -> None:
    attachments = [{"filename": f"file-{idx}.pdf", "text": "text"} for idx in range(10)]
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered = _render(attachments=attachments, insight_digest=digest)
    assert len(rendered.splitlines()) <= 18


def test_premium_clarity_extraction_failed_placeholder() -> None:
    rendered = _render(
        attachments=[{"filename": "file.pdf", "text": ""}],
        body_summary="",
        extraction_failed=True,
        confidence_percent=30,
    )
    assert "• file.pdf" in rendered


def test_premium_clarity_attachments_absent() -> None:
    rendered = _render(attachments=[])
    assert "📎 Вложения (0):" in rendered


def test_premium_clarity_attachments_visible() -> None:
    attachments = [
        {"filename": "one.pdf", "text": "text"},
        {"filename": "two.pdf", "text": ""},
        {"filename": "three.pdf", "text": ""},
    ]
    rendered = _render(attachments=attachments)
    lines = rendered.splitlines()
    assert "📎 Вложения (3):" in lines
    assert "• one.pdf" in lines
    assert "• two.pdf" in lines
    assert "• three.pdf" in lines


def test_premium_clarity_attachments_more_than_three() -> None:
    attachments = [{"filename": f"file-{idx}.pdf", "text": ""} for idx in range(6)]
    rendered = _render(attachments=attachments)
    lines = rendered.splitlines()
    assert "📎 Вложения (6):" in lines
    assert "• file-0.pdf" in lines
    assert "• file-1.pdf" in lines
    assert "• file-2.pdf" in lines
    assert any("... и ещё 3" in line for line in lines)


def test_premium_clarity_attachment_summary_status() -> None:
    attachments = [{"filename": "invoice.pdf", "text": ""}]
    summaries = [{"filename": "invoice.pdf", "summary": "Счёт"}]
    rendered = _render(attachments=attachments, attachment_summaries=summaries)
    assert "• invoice.pdf — Счёт" in rendered


def test_premium_clarity_attachment_summary_suppresses_numbers() -> None:
    attachments = [{"filename": "invoice_1234.pdf", "text": ""}]
    summaries = [
        {"filename": "invoice_1234.pdf", "summary": "Счёт №1234 на 50 000₽"}
    ]
    rendered = _render(
        attachments=attachments,
        attachment_summaries=summaries,
        extraction_failed=True,
        confidence_available=False,
        confidence_percent=0,
        body_summary="",
        body_text="",
    )
    line = next(
        line for line in rendered.splitlines() if line.startswith("• invoice_1234.pdf")
    )
    summary = line.split("—", 1)[-1].strip()
    assert "invoice_1234.pdf" in line
    assert "Счёт" in summary
    assert re.search(r"\d", summary) is None


def test_premium_clarity_fact_provenance_subject() -> None:
    rendered = _render(
        subject="Счет на 12 000 ₽",
        body_summary="Проверьте счет.",
        body_text="",
    )
    assert "Сумма: 12 000 ₽ (тема)" in rendered


def test_premium_clarity_fact_provenance_body() -> None:
    rendered = _render(
        body_summary="Срок оплаты указан.",
        body_text="Оплатить до 12.02.2025.",
    )
    assert "Дата: 12.02.2025 (письмо)" in rendered


def test_premium_clarity_fact_provenance_attachment() -> None:
    attachments = [{"filename": "invoice.pdf", "text": "Сумма 5 000 руб."}]
    rendered = _render(
        attachments=attachments,
        body_summary="Есть счет.",
        body_text="",
    )
    assert "Сумма: 5 000 руб. (invoice.pdf)" in rendered


def test_premium_clarity_fact_provenance_omitted_for_ambiguous_attachment() -> None:
    attachments = [
        {"filename": "first.pdf", "text": "Сумма 5 000 руб."},
        {"filename": "second.pdf", "text": "Сумма 5 000 руб."},
    ]
    rendered = _render(
        attachments=attachments,
        body_summary="Есть счет.",
        body_text="",
    )
    assert "(first.pdf)" not in rendered
    assert "(second.pdf)" not in rendered


def test_premium_clarity_fact_provenance_omitted_when_unknown() -> None:
    rendered = _render(body_summary="Общий обзор.", body_text="Без деталей.")
    assert "(тема)" not in rendered
    assert "(письмо)" not in rendered
    assert "(invoice.pdf)" not in rendered


def test_premium_clarity_subject_numbers_tagged_and_essence_generic() -> None:
    rendered = _render(
        subject="Счет на 12 000 ₽",
        body_summary="Оплатить 12 000 ₽ до 12.02.2025.",
        body_text="Оплатить до 12.02.2025.",
    )
    lines = rendered.splitlines()
    assert lines[0].startswith("🔵 ")
    assert not any(char.isdigit() for char in lines[0])
    assert "Тема: Счет на 12 000 ₽ (тема)" in rendered


def test_premium_clarity_extraction_failed_suppresses_numeric_facts() -> None:
    rendered = _render(
        subject="Без чисел",
        body_summary="Сумма 5 000 руб.",
        body_text="Сумма 5 000 руб.",
        attachments=[{"filename": "file.pdf", "text": "Сумма 5 000 руб."}],
        extraction_failed=True,
        confidence_percent=20,
        confidence_available=False,
    )
    assert "Сумма:" not in rendered
    assert "Проверьте вручную" in rendered


def test_premium_clarity_why_line_for_low_confidence() -> None:
    rendered = _render(confidence_percent=30, confidence_dots_threshold=75)
    why_lines = [line for line in rendered.splitlines() if line.startswith("Почему:")]
    assert len(why_lines) == 1


def test_premium_clarity_why_line_for_deadline() -> None:
    commitments = [
        Commitment(
            commitment_text="Согласовать",
            deadline_iso="2026-01-03",
            status="pending",
            source="heuristic",
            confidence=0.9,
        )
    ]
    rendered = _render(
        commitments=commitments,
        received_at=datetime(2026, 1, 1),
    )
    assert any(line.startswith("Почему:") for line in rendered.splitlines())


def test_premium_clarity_why_line_for_high_risk() -> None:
    rendered = _render(
        insights=[
            processor.Insight(
                type="High-Risk Window",
                severity="HIGH",
                explanation="",
                recommendation="",
            )
        ]
    )
    assert any(line.startswith("Почему:") for line in rendered.splitlines())


def test_premium_clarity_why_line_absent_when_not_needed() -> None:
    rendered = _render(confidence_percent=90, confidence_dots_threshold=75)
    assert all(
        not line.startswith("Почему:") for line in rendered.splitlines()
    )


def test_premium_clarity_line_budget_with_extraction_failure_and_low_confidence() -> None:
    attachments = [{"filename": f"file-{idx}.pdf", "text": ""} for idx in range(5)]
    rendered = _render(
        attachments=attachments,
        extraction_failed=True,
        confidence_percent=10,
        confidence_dots_threshold=75,
        body_summary="",
        body_text="",
    )
    assert len(rendered.splitlines()) <= 18


def test_premium_clarity_confidence_dots_never_mode() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered = _render(
        insight_digest=digest,
        confidence_percent=20,
        confidence_dots_mode="never",
    )
    assert _extract_dots(rendered) == ""


def test_premium_clarity_confidence_dots_auto_above_threshold() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered = _render(
        insight_digest=digest,
        confidence_percent=90,
        confidence_dots_mode="auto",
        confidence_dots_threshold=75,
    )
    assert _extract_dots(rendered) == ""


def test_premium_clarity_confidence_dots_auto_below_threshold() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered = _render(
        insight_digest=digest,
        confidence_percent=30,
        confidence_dots_mode="auto",
        confidence_dots_threshold=75,
    )
    dots = _extract_dots(rendered)
    assert len(dots) == 10


def test_premium_clarity_confidence_dots_always_mode() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    rendered = _render(
        insight_digest=digest,
        confidence_percent=85,
        confidence_available=True,
        confidence_dots_mode="always",
    )
    dots = _extract_dots(rendered)
    assert len(dots) == 10


def test_premium_clarity_confidence_dots_scale_five() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    cases = [
        (0, 0),
        (20, 1),
        (40, 2),
        (60, 3),
        (80, 4),
        (100, 5),
    ]
    for percent, filled in cases:
        rendered = _render(
            insight_digest=digest,
            confidence_percent=percent,
            confidence_available=True,
            confidence_dots_mode="always",
            confidence_dots_scale=5,
        )
        dots = _extract_dots(rendered)
        assert dots == ("●" * filled + "○" * (5 - filled))


def test_premium_clarity_confidence_dots_scale_ten() -> None:
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="Есть просрочки.",
    )
    cases = [
        (0, 0),
        (50, 5),
        (100, 10),
    ]
    for percent, filled in cases:
        rendered = _render(
            insight_digest=digest,
            confidence_percent=percent,
            confidence_available=True,
            confidence_dots_mode="always",
            confidence_dots_scale=10,
        )
        dots = _extract_dots(rendered)
        assert dots == ("●" * filled + "○" * (10 - filled))


def test_premium_clarity_line_budget_enforced_and_html_valid() -> None:
    attachments = [{"filename": f"file-{idx}.pdf", "text": ""} for idx in range(6)]
    digest = InsightDigest(
        headline="Контакт в зоне риска.",
        status_label="Risk Zone",
        short_explanation="\n".join(f"Подробность {idx}" for idx in range(10)),
    )
    rendered = _render(
        attachments=attachments,
        attachment_summaries=[{"filename": "file-0.pdf", "summary": "Счёт"}],
        insight_digest=digest,
        confidence_percent=10,
        confidence_dots_mode="always",
        subject="Очень длинная тема <с тегами> " * 3,
    )
    lines = rendered.splitlines()
    assert len(lines) <= 18
    assert lines[0].startswith("🔵 ")
    assert any(line.startswith("От: ") for line in lines)
    assert any(line.startswith("Тема: ") for line in lines)
    assert any(line.startswith("📎 Вложения (6):") for line in lines)
    assert "• file-0.pdf" in rendered
    assert "• file-1.pdf" in rendered
    assert "• file-2.pdf" in rendered
    assert rendered.count("<tg-spoiler>") == rendered.count("</tg-spoiler>")
    assert "<с тегами>" not in rendered
