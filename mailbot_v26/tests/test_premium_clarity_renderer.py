from __future__ import annotations

from mailbot_v26.insights.digest import InsightDigest
from mailbot_v26.pipeline import processor


def _render(
    *,
    priority: str = "🔵",
    attachments: list[dict[str, object]] | None = None,
    body_summary: str = "Краткий факт письма.",
    extraction_failed: bool = False,
    confidence_percent: int = 80,
    confidence_available: bool = True,
    confidence_dots_mode: str = "auto",
    confidence_dots_threshold: int = 75,
    insight_digest: InsightDigest | None = None,
) -> str:
    return processor._build_premium_clarity_text(
        priority=priority,
        from_email="sender@example.com",
        from_name="Sender",
        subject="Тема письма",
        action_line="Ответить клиенту",
        body_summary=body_summary,
        attachments=attachments or [],
        insights=[],
        insight_digest=insight_digest,
        commitments=[],
        attachments_count=len(attachments or []),
        extracted_text_len=0 if extraction_failed else 120,
        confidence_percent=confidence_percent,
        confidence_available=confidence_available,
        confidence_dots_mode=confidence_dots_mode,
        confidence_dots_threshold=confidence_dots_threshold,
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
    assert any("... ещё 1" in line for line in lines)


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
    assert "не извлечено" in rendered


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
