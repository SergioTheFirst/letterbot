from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from mailbot_v26.text.mojibake import normalize_mojibake_text


@dataclass(frozen=True, slots=True)
class DocumentTemplate:
    id: str
    priority: int
    sender_domains: tuple[str, ...] = ()
    sender_emails: tuple[str, ...] = ()
    subject_keywords: tuple[str, ...] = ()
    attachment_keywords: tuple[str, ...] = ()
    required_anchors: tuple[str, ...] = ()
    forbidden_anchors: tuple[str, ...] = ()
    preferred_amount_anchors: tuple[str, ...] = ()
    preferred_due_date_anchors: tuple[str, ...] = ()
    doc_kind_override: str | None = None
    action_hint: str | None = None
    action_override: str | None = None
    confidence_boost: float = 0.0
    strong_suppression_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DocumentTemplateMatch:
    template: DocumentTemplate
    score: float
    required_hits: int
    subject_hits: int
    attachment_hits: int
    sender_match: bool
    preferred_amount_anchor_hit: bool
    preferred_due_date_anchor_hit: bool
    strong_match: bool


def _normalize_text(value: str | None) -> str:
    normalized = normalize_mojibake_text(str(value or "")).strip().casefold()
    return " ".join(normalized.split())


def _count_hits(text: str, needles: Sequence[str]) -> int:
    if not text:
        return 0
    return sum(1 for token in needles if token and token in text)


def _sender_match(template: DocumentTemplate, sender_email: str) -> bool:
    normalized_sender = _normalize_text(sender_email)
    if not normalized_sender:
        return False
    sender_emails = {_normalize_text(item) for item in template.sender_emails if item}
    if normalized_sender in sender_emails:
        return True
    if "@" not in normalized_sender:
        return False
    sender_domain = normalized_sender.split("@", 1)[1]
    sender_domains = {
        _normalize_text(item) for item in template.sender_domains if item
    }
    return sender_domain in sender_domains


def select_document_template(
    *,
    sender_email: str,
    subject: str,
    body_text: str,
    attachment_names: Iterable[str] = (),
    attachment_text: str = "",
    templates: Sequence[DocumentTemplate] | None = None,
) -> DocumentTemplateMatch | None:
    candidates = tuple(templates or BUILTIN_DOCUMENT_TEMPLATES)
    if not candidates:
        return None

    subject_text = _normalize_text(subject)
    body = _normalize_text(body_text)
    attachment_name_text = _normalize_text(
        " ".join(str(item or "") for item in attachment_names)
    )
    attachment_body_text = _normalize_text(attachment_text)
    combined = " ".join(
        part
        for part in (subject_text, body, attachment_name_text, attachment_body_text)
        if part
    )

    best_match: DocumentTemplateMatch | None = None
    for template in candidates:
        if _count_hits(combined, template.forbidden_anchors) > 0:
            continue
        required_hits = _count_hits(combined, template.required_anchors)
        subject_hits = _count_hits(subject_text, template.subject_keywords)
        attachment_hits = _count_hits(
            " ".join(part for part in (attachment_name_text, attachment_body_text) if part),
            template.attachment_keywords,
        )
        content_hits = required_hits + subject_hits + attachment_hits
        if content_hits <= 0:
            continue
        if template.required_anchors and required_hits <= 0:
            continue

        sender_hit = _sender_match(template, sender_email)
        preferred_amount_hit = _count_hits(combined, template.preferred_amount_anchors) > 0
        preferred_due_hit = _count_hits(combined, template.preferred_due_date_anchors) > 0
        score = float(content_hits * 3 + required_hits * 2 + subject_hits + attachment_hits)
        if sender_hit:
            score += 2.0
        if preferred_amount_hit:
            score += 1.0
        if preferred_due_hit:
            score += 1.0
        score += template.priority / 100.0

        strong_match = bool(required_hits >= 1 and (subject_hits + attachment_hits) >= 1)
        if sender_hit and required_hits >= 1:
            strong_match = True
        if content_hits >= 4:
            strong_match = True

        match = DocumentTemplateMatch(
            template=template,
            score=score,
            required_hits=required_hits,
            subject_hits=subject_hits,
            attachment_hits=attachment_hits,
            sender_match=sender_hit,
            preferred_amount_anchor_hit=preferred_amount_hit,
            preferred_due_date_anchor_hit=preferred_due_hit,
            strong_match=strong_match,
        )
        if best_match is None:
            best_match = match
            continue
        if match.score > best_match.score:
            best_match = match
            continue
        if (
            match.score == best_match.score
            and match.template.priority > best_match.template.priority
        ):
            best_match = match

    return best_match


BUILTIN_DOCUMENT_TEMPLATES: tuple[DocumentTemplate, ...] = (
    DocumentTemplate(
        id="russian_payroll_common",
        priority=120,
        sender_domains=("hr.vendor.test",),
        subject_keywords=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0432\u0435\u0434\u043e\u043c\u043e\u0441\u0442\u044c",
            "payroll",
        ),
        attachment_keywords=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0432\u0435\u0434\u043e\u043c\u043e\u0441\u0442\u044c",
        ),
        required_anchors=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e",
            "\u0443\u0434\u0435\u0440\u0436\u0430\u043d\u043e",
            "\u043a \u0432\u044b\u043f\u043b\u0430\u0442\u0435",
        ),
        forbidden_anchors=(
            "\u0430\u043a\u0442 \u0441\u0432\u0435\u0440\u043a\u0438",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0435\u0442\u043e\u0432",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0451\u0442\u043e\u0432",
        ),
        doc_kind_override="payroll",
        action_override="\u041e\u0437\u043d\u0430\u043a\u043e\u043c\u0438\u0442\u044c\u0441\u044f",
        confidence_boost=0.12,
        strong_suppression_flags=("suppress_invoice_payment_action",),
    ),
    DocumentTemplate(
        id="russian_reconciliation_common",
        priority=110,
        sender_domains=("reconciliation.vendor.test",),
        subject_keywords=(
            "\u0430\u043a\u0442 \u0441\u0432\u0435\u0440\u043a\u0438",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0435\u0442\u043e\u0432",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0451\u0442\u043e\u0432",
        ),
        attachment_keywords=(
            "\u0430\u043a\u0442 \u0441\u0432\u0435\u0440\u043a\u0438",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0435\u0442\u043e\u0432",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0451\u0442\u043e\u0432",
        ),
        required_anchors=(
            "\u0430\u043a\u0442 \u0441\u0432\u0435\u0440\u043a\u0438",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0435\u0442\u043e\u0432",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0451\u0442\u043e\u0432",
        ),
        forbidden_anchors=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0432\u0435\u0434\u043e\u043c\u043e\u0441\u0442\u044c",
        ),
        doc_kind_override="reconciliation",
        action_override="\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c",
        confidence_boost=0.1,
        strong_suppression_flags=("suppress_invoice_payment_action",),
    ),
    DocumentTemplate(
        id="russian_invoice_common",
        priority=100,
        sender_domains=("billing.vendor.test",),
        subject_keywords=(
            "\u0441\u0447\u0435\u0442",
            "\u0441\u0447\u0451\u0442",
            "invoice",
        ),
        attachment_keywords=(
            "\u0441\u0447\u0435\u0442",
            "\u0441\u0447\u0451\u0442",
            "invoice",
        ),
        required_anchors=(
            "\u0441\u0447\u0435\u0442",
            "\u0441\u0447\u0451\u0442",
            "invoice",
            "\u043a \u043e\u043f\u043b\u0430\u0442\u0435",
            "\u0438\u0442\u043e\u0433\u043e",
            "\u0441\u0443\u043c\u043c\u0430 \u043a \u043e\u043f\u043b\u0430\u0442\u0435",
            "amount due",
            "invoice total",
            "total payable",
        ),
        forbidden_anchors=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0432\u0435\u0434\u043e\u043c\u043e\u0441\u0442\u044c",
            "\u0430\u043a\u0442 \u0441\u0432\u0435\u0440\u043a\u0438",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0435\u0442\u043e\u0432",
            "\u0432\u0437\u0430\u0438\u043c\u043d\u044b\u0445 \u0440\u0430\u0441\u0447\u0451\u0442\u043e\u0432",
        ),
        preferred_amount_anchors=(
            "\u043a \u043e\u043f\u043b\u0430\u0442\u0435",
            "\u0438\u0442\u043e\u0433\u043e",
            "\u0441\u0443\u043c\u043c\u0430 \u043a \u043e\u043f\u043b\u0430\u0442\u0435",
            "amount due",
            "invoice total",
            "total payable",
        ),
        preferred_due_date_anchors=(
            "\u043e\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u0434\u043e",
            "\u0441\u0440\u043e\u043a \u043e\u043f\u043b\u0430\u0442\u044b",
            "due date",
            "payment due",
            "\u0443\u043f\u043b\u0430\u0442\u0438\u0442\u044c \u0434\u043e",
        ),
        doc_kind_override="invoice",
        action_hint="\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c",
        confidence_boost=0.08,
    ),
    DocumentTemplate(
        id="contract_or_amendment_common",
        priority=95,
        sender_domains=("legal.vendor.test",),
        subject_keywords=(
            "\u0434\u043e\u0433\u043e\u0432\u043e\u0440",
            "contract",
            "\u0441\u043e\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435",
            "amendment",
            "\u0434\u043e\u043f\u0441\u043e\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435",
        ),
        attachment_keywords=(
            "\u0434\u043e\u0433\u043e\u0432\u043e\u0440",
            "contract",
            "agreement",
            "amendment",
            "\u0434\u043e\u043f\u0441\u043e\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435",
        ),
        required_anchors=(
            "\u0434\u043e\u0433\u043e\u0432\u043e\u0440",
            "contract",
            "\u0441\u043e\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435",
            "amendment",
            "\u0434\u043e\u043f\u0441\u043e\u0433\u043b\u0430\u0448\u0435\u043d\u0438\u0435",
        ),
        forbidden_anchors=(
            "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0440\u0430\u0441\u0447\u0451\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
            "\u0432\u0435\u0434\u043e\u043c\u043e\u0441\u0442\u044c",
        ),
        doc_kind_override="contract",
        action_hint="\u041f\u0440\u043e\u0432\u0435\u0440\u0438\u0442\u044c \u0434\u043e\u0433\u043e\u0432\u043e\u0440",
        confidence_boost=0.08,
        strong_suppression_flags=("suppress_invoice_payment_action",),
    ),
)


__all__ = [
    "BUILTIN_DOCUMENT_TEMPLATES",
    "DocumentTemplate",
    "DocumentTemplateMatch",
    "select_document_template",
]
