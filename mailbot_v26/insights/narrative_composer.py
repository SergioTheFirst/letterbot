from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier
from mailbot_v26.facts.fact_extractor import FactExtractor
from mailbot_v26.insights.commitment_tracker import Commitment, extract_deadline_ru
from mailbot_v26.observability import get_logger
from mailbot_v26.storage.analytics import KnowledgeAnalytics

logger = get_logger("mailbot")

FACT_LINE_LIMIT = 180
PATTERN_LINE_LIMIT = 180
ACTION_LINE_LIMIT = 180
MIN_PATTERN_SAMPLES = 3
FREQUENCY_SPIKE_RATIO = 3.0
RESPONSE_SLOWDOWN_RATIO = 1.5


@dataclass(frozen=True, slots=True)
class NarrativeResult:
    fact: str
    pattern: str | None
    action: str | None
    reasons: tuple[str, ...]


def compose_narrative(
    *,
    email_id: int | None,
    subject: str,
    body_text: str,
    from_email: str,
    mail_type: str,
    received_at: datetime,
    attachments: list[dict[str, object]],
    entity_id: str | None,
    analytics: KnowledgeAnalytics | None,
    commitments: Iterable[Commitment] | None = None,
    enable_patterns: bool = True,
) -> NarrativeResult | None:
    try:
        result = _compose_narrative(
            subject=subject,
            body_text=body_text,
            from_email=from_email,
            mail_type=mail_type,
            received_at=received_at,
            attachments=attachments,
            entity_id=entity_id,
            analytics=analytics,
            commitments=commitments,
            enable_patterns=enable_patterns,
        )
    except Exception as exc:
        logger.error(
            "narrative_failed",
            email_id=email_id,
            error_class=exc.__class__.__name__,
            error=str(exc),
        )
        return None

    if result is None:
        return None

    logger.info(
        "narrative_composed",
        email_id=email_id,
        fact_present=bool(result.fact),
        pattern_present=bool(result.pattern),
        action_present=bool(result.action),
        reasons_count=len(result.reasons),
    )
    return result


def _compose_narrative(
    *,
    subject: str,
    body_text: str,
    from_email: str,
    mail_type: str,
    received_at: datetime,
    attachments: list[dict[str, object]],
    entity_id: str | None,
    analytics: KnowledgeAnalytics | None,
    commitments: Iterable[Commitment] | None,
    enable_patterns: bool,
) -> NarrativeResult | None:
    combined_text = f"{subject or ''}\n{body_text or ''}".strip()
    facts = FactExtractor().extract_facts(combined_text)
    reasons: list[str] = []

    fact_parts: list[str] = []
    if from_email:
        fact_parts.append(f"От: {from_email}")
        reasons.append("fact.sender")
    normalized_type = (mail_type or "").strip().upper()
    if normalized_type:
        fact_parts.append(f"Тип: {normalized_type}")
        reasons.append("fact.type")
    amount = _select_amount(facts.amounts)
    if amount:
        fact_parts.append(f"Сумма: {amount}")
        reasons.append("fact.amount")
    deadline = _select_deadline(combined_text, commitments)
    if deadline:
        fact_parts.append(f"Дедлайн: {deadline}")
        reasons.append("fact.deadline")
    urgency = _find_urgency(combined_text)
    if urgency:
        fact_parts.append(f"Срочно: {urgency}")
        reasons.append("fact.urgency")
    attachment_summary = _attachment_summary(attachments)
    if attachment_summary:
        fact_parts.append(attachment_summary)
        reasons.append("fact.attachments")
    chain_line = _chain_summary(entity_id, analytics)
    if chain_line:
        fact_parts.append(chain_line)
        reasons.append("fact.chain")
    if not fact_parts:
        return None

    fact_line = _trim_line("; ".join(fact_parts), FACT_LINE_LIMIT)
    pattern_line = None
    if enable_patterns:
        pattern_line = _build_pattern_line(entity_id, analytics)
    action_line = _build_action_line(
        mail_type=normalized_type,
        deadline_iso=deadline,
        reference_date=received_at.date(),
    )

    return NarrativeResult(
        fact=fact_line,
        pattern=_trim_line(pattern_line, PATTERN_LINE_LIMIT) if pattern_line else None,
        action=_trim_line(action_line, ACTION_LINE_LIMIT) if action_line else None,
        reasons=tuple(reasons),
    )


def _select_amount(amounts: list[str]) -> str | None:
    parsed = [_parse_amount(value) for value in amounts]
    numbers = [value for value in parsed if value is not None]
    if not numbers:
        return amounts[0] if amounts else None
    max_index = numbers.index(max(numbers))
    return amounts[max_index] if max_index < len(amounts) else amounts[0]


def _parse_amount(value: str) -> float | None:
    if not value:
        return None
    cleaned = value.replace("\u00A0", " ").replace(" ", "")
    cleaned = cleaned.replace(",", ".")
    digits = "".join(ch for ch in cleaned if ch.isdigit() or ch == ".")
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None


def _select_deadline(
    text: str,
    commitments: Iterable[Commitment] | None,
) -> str | None:
    earliest: str | None = None
    if commitments:
        for commitment in commitments:
            if not commitment.deadline_iso:
                continue
            if earliest is None or commitment.deadline_iso < earliest:
                earliest = commitment.deadline_iso
    if earliest:
        return earliest
    return extract_deadline_ru(text or "")


def _find_urgency(text: str) -> str | None:
    lowered = (text or "").lower()
    for keyword in MailTypeClassifier.URGENCY_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def _attachment_summary(attachments: list[dict[str, object]]) -> str | None:
    if not attachments:
        return None
    types = sorted({_attachment_type_label(str(a.get("filename") or "")) for a in attachments})
    types = [label for label in types if label]
    if types:
        return f"Вложения: {len(attachments)} ({', '.join(types)})"
    return f"Вложения: {len(attachments)}"


def _attachment_type_label(filename: str) -> str:
    cleaned = (filename or "").strip()
    if "." not in cleaned:
        return "OTHER"
    ext = cleaned.rsplit(".", 1)[-1].lower()
    ext = "".join(ch for ch in ext if ch.isalnum())
    if not ext:
        return "OTHER"
    return {"jpeg": "JPG", "jpg": "JPG"}.get(ext, ext.upper())


def _chain_summary(entity_id: str | None, analytics: KnowledgeAnalytics | None) -> str | None:
    if not entity_id or analytics is None:
        return None
    count = analytics.interaction_event_count(
        entity_id=entity_id,
        event_type="email_received",
        days=14,
    )
    if count <= 1:
        return None
    return f"Цепочка: {count} писем за 14д"


def _build_pattern_line(
    entity_id: str | None,
    analytics: KnowledgeAnalytics | None,
) -> str | None:
    if not entity_id or analytics is None:
        return None
    frequency = analytics.interaction_event_counts(
        entity_id=entity_id,
        event_type="email_received",
        recent_days=7,
        previous_days=7,
    )
    recent = frequency["recent"]
    previous = frequency["previous"]
    if recent >= MIN_PATTERN_SAMPLES and previous >= MIN_PATTERN_SAMPLES:
        if previous > 0 and (recent / previous) >= FREQUENCY_SPIKE_RATIO:
            return f"Обычно {previous}/нед, сейчас {recent}/нед."

    now = datetime.now(timezone.utc)
    current = analytics.get_avg_response_time(entity_id=entity_id, window=14, end_dt=now)
    baseline_end = now - timedelta(days=14)
    baseline = analytics.get_avg_response_time(
        entity_id=entity_id,
        window=14,
        end_dt=baseline_end,
    )
    if (
        current["avg_hours"] is not None
        and baseline["avg_hours"] is not None
        and current["sample_size"] >= MIN_PATTERN_SAMPLES
        and baseline["sample_size"] >= MIN_PATTERN_SAMPLES
        and current["avg_hours"] >= baseline["avg_hours"] * RESPONSE_SLOWDOWN_RATIO
    ):
        return (
            f"Ответы стали медленнее: раньше {baseline['avg_hours']:.1f}ч, "
            f"сейчас {current['avg_hours']:.1f}ч."
        )
    return None


def _build_action_line(
    *,
    mail_type: str,
    deadline_iso: str | None,
    reference_date: date,
) -> str | None:
    now = reference_date
    if deadline_iso:
        try:
            deadline_date = datetime.fromisoformat(deadline_iso).date()
        except ValueError:
            deadline_date = None
        if deadline_date is not None:
            delta_days = (deadline_date - now).days
            if delta_days <= 1:
                return f"Ответить сегодня и подтвердить дедлайн {deadline_date.isoformat()}."
            if delta_days <= 3:
                return f"Согласовать действия до {deadline_date.isoformat()}."

    if mail_type == "INVOICE_FINAL":
        return "Проверить оплату, срок и реквизиты счёта."
    if mail_type in {"CONTRACT_TERMINATION", "CONTRACT_AMENDMENT"}:
        return "Проверить условия и ответить юридической команде."
    if mail_type == "REMINDER_ESCALATION":
        return "Ответить с подтверждением срока исполнения."
    return None


def _trim_line(value: str | None, limit: int) -> str | None:
    if not value:
        return None
    cleaned = " ".join(value.split())
    if len(cleaned) <= limit:
        return cleaned
    trimmed = cleaned[: limit - 1].rstrip()
    return f"{trimmed}…"


__all__ = ["NarrativeResult", "compose_narrative"]
