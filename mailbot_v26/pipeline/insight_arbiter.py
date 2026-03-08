from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.observability import get_logger

logger = get_logger("mailbot")

_MIN_SUMMARY_WORDS = 2
_MIN_SUMMARY_CHARS = 12
_SUMMARY_PLACEHOLDER_PATTERNS = (
    "проверить письмо",
    "проверь письмо",
    "check email",
    "check mail",
)
_LOW_SIGNAL_CONTAINS = (
    "см вложение",
    "смотри вложение",
    "смотреть вложение",
    "см. вложение",
    "см.вложение",
)


@dataclass(frozen=True, slots=True)
class InsightArbiterInput:
    llm_summary: str
    extracted_text_len: int
    attachment_details: list[dict[str, Any]]
    commitments: list[Commitment]
    email_id: int | None = None


@dataclass(frozen=True, slots=True)
class InsightArbiterResult:
    summary: str
    replaced: bool
    reason: str


def _normalize_summary_text(summary: str) -> str:
    normalized = "".join(
        char if char.isalnum() or char.isspace() else " "
        for char in (summary or "").lower()
    )
    return " ".join(normalized.split())


def _is_summary_placeholder(summary: str) -> bool:
    normalized = _normalize_summary_text(summary)
    if not normalized:
        return True
    for pattern in _SUMMARY_PLACEHOLDER_PATTERNS:
        if normalized == pattern:
            return True
        if normalized.startswith(pattern) and len(normalized.split()) <= 4:
            return True
    return False


def _is_low_signal_summary(summary: str) -> bool:
    normalized = _normalize_summary_text(summary)
    if _is_summary_placeholder(summary):
        return True
    if len((summary or "").strip()) < _MIN_SUMMARY_CHARS:
        return True
    words = normalized.split()
    if len(words) < _MIN_SUMMARY_WORDS:
        return True
    if any(phrase in normalized for phrase in _LOW_SIGNAL_CONTAINS):
        return True
    return False


def _attachment_description(details: list[dict[str, Any]]) -> str:
    count = len(details)
    kinds = sorted({detail.get("kind") for detail in details if detail.get("kind")})
    total_chars = sum(int(detail.get("chars") or 0) for detail in details)
    kind_part = f" ({', '.join(kinds)})" if kinds else ""
    text_part = f"; текст во вложениях: {total_chars} символов" if total_chars else ""
    return f"{count} вложений{kind_part}{text_part}"


def _build_fallback_summary(
    *,
    extracted_text_len: int,
    attachment_details: list[dict[str, Any]],
) -> str:
    attachments_count = len(attachment_details)
    if extracted_text_len <= 0 and attachments_count == 0:
        return (
            "Не удалось извлечь текст письма или вложений; требуется ручной просмотр."
        )
    prefix = "Автоматическая сводка слишком общая."
    if attachments_count > 0 and extracted_text_len <= 0:
        description = _attachment_description(attachment_details)
        return f"{prefix} Письмо содержит {description}, текст письма не извлечён."
    if attachments_count == 0:
        return (
            f"{prefix} В письме {extracted_text_len} символов текста; "
            "требуется ручной просмотр."
        )
    description = _attachment_description(attachment_details)
    return f"{prefix} В письме {extracted_text_len} символов текста и {description}."


def apply_insight_arbiter(payload: InsightArbiterInput) -> InsightArbiterResult:
    commitments_count = len(payload.commitments)
    deadlines_count = sum(
        1 for commitment in payload.commitments if commitment.deadline_iso
    )
    if commitments_count > 0:
        logger.info(
            "[INSIGHT-ARBITER] preserved_summary",
            email_id=payload.email_id,
            reason="commitments_present",
            commitments_count=commitments_count,
            deadlines_count=deadlines_count,
        )
        return InsightArbiterResult(
            summary=payload.llm_summary,
            replaced=False,
            reason="commitments_present",
        )

    if not _is_low_signal_summary(payload.llm_summary):
        logger.info(
            "[INSIGHT-ARBITER] preserved_summary",
            email_id=payload.email_id,
            reason="summary_ok",
            commitments_count=commitments_count,
            deadlines_count=deadlines_count,
        )
        return InsightArbiterResult(
            summary=payload.llm_summary,
            replaced=False,
            reason="summary_ok",
        )

    fallback = _build_fallback_summary(
        extracted_text_len=payload.extracted_text_len,
        attachment_details=payload.attachment_details,
    )
    reason = (
        "extraction_failed"
        if payload.extracted_text_len <= 0 and not payload.attachment_details
        else "summary_low_signal"
    )
    logger.info(
        "[INSIGHT-ARBITER] replaced_summary",
        email_id=payload.email_id,
        reason=reason,
        commitments_count=commitments_count,
        deadlines_count=deadlines_count,
    )
    return InsightArbiterResult(summary=fallback, replaced=True, reason=reason)
