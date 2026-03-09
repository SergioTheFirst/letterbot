"""Deterministic analysis helper for correction-driven template promotion."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mailbot_v26.domain.issuer_identity import (
    build_issuer_fingerprint,
    normalize_sender_identity,
)

_DOC_KIND_TO_TEMPLATE_ID = {
    "invoice": "russian_invoice_common",
    "payroll": "russian_payroll_common",
    "reconciliation": "russian_reconciliation_common",
    "contract": "contract_or_amendment_common",
}


@dataclass(frozen=True, slots=True)
class TemplatePromotionSignal:
    account_id: str
    scope_kind: str
    scope_value: str
    doc_kind: str
    template_id: str
    issuer_fingerprint: str
    identity_confidence: str
    correction_count: int
    dominant_priority: str
    consistency_ratio: float
    strength: float
    sender_emails: tuple[str, ...]
    email_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class RuntimePromotionPolicy:
    min_corrections: int
    min_consistency: float
    min_strength: float
    confidence_boost: float


@dataclass(frozen=True, slots=True)
class RuntimeTemplatePromotion:
    signal: TemplatePromotionSignal
    confidence_boost: float


_RUNTIME_PROMOTION_POLICIES = {
    "invoice": RuntimePromotionPolicy(
        min_corrections=5,
        min_consistency=0.85,
        min_strength=0.85,
        confidence_boost=0.08,
    ),
    "payroll": RuntimePromotionPolicy(
        min_corrections=4,
        min_consistency=0.8,
        min_strength=0.8,
        confidence_boost=0.06,
    ),
    "reconciliation": RuntimePromotionPolicy(
        min_corrections=4,
        min_consistency=0.8,
        min_strength=0.8,
        confidence_boost=0.06,
    ),
    "contract": RuntimePromotionPolicy(
        min_corrections=3,
        min_consistency=0.75,
        min_strength=0.75,
        confidence_boost=0.05,
    ),
}

_RUNTIME_SIGNAL_CACHE: dict[str, tuple[int, tuple[TemplatePromotionSignal, ...]]] = {}


def _safe_json_loads(raw: Any) -> dict[str, Any]:
    if raw in (None, ""):
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError):
        return {}


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _suggest_template_id(doc_kind: str) -> str | None:
    return _DOC_KIND_TO_TEMPLATE_ID.get(_normalize_text(doc_kind))


def clear_runtime_template_promotion_cache() -> None:
    _RUNTIME_SIGNAL_CACHE.clear()


def _runtime_signal_candidates(
    db_path: Path | str,
) -> tuple[TemplatePromotionSignal, ...]:
    path = Path(db_path)
    cache_key = str(path.resolve())
    try:
        mtime_ns = int(path.stat().st_mtime_ns)
    except OSError:
        mtime_ns = -1
    cached = _RUNTIME_SIGNAL_CACHE.get(cache_key)
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]
    signals = analyze_template_promotion_candidates(
        path,
        min_corrections=1,
        min_consistency=0.0,
        min_distinct_senders_for_domain=2,
    )
    _RUNTIME_SIGNAL_CACHE[cache_key] = (mtime_ns, signals)
    return signals


def find_runtime_template_promotion(
    db_path: Path | str,
    *,
    account_id: str,
    sender_email: str,
    doc_kind: str,
    display_name: str = "",
    subject_hint: str = "",
    doc_marker: str = "",
) -> tuple[RuntimeTemplatePromotion | None, str | None]:
    normalized_account = str(account_id or "").strip()
    normalized_sender = _normalize_text(sender_email)
    normalized_doc_kind = _normalize_text(doc_kind)
    if not normalized_account or not normalized_sender or not normalized_doc_kind:
        return None, "missing_scope"
    identity = normalize_sender_identity(
        normalized_sender,
        display_name=display_name,
        subject_hint=subject_hint,
        doc_marker=doc_marker,
    )
    if str(identity.get("confidence") or "").strip().lower() == "weak":
        return None, "weak_identity"
    policy = _RUNTIME_PROMOTION_POLICIES.get(normalized_doc_kind)
    if policy is None:
        return None, "unsupported_doc_kind"
    issuer_fingerprint = build_issuer_fingerprint(identity, normalized_doc_kind)
    sender_domain = (
        normalized_sender.split("@", 1)[1] if "@" in normalized_sender else ""
    )
    matches = [
        signal
        for signal in _runtime_signal_candidates(db_path)
        if signal.account_id == normalized_account
        and signal.doc_kind == normalized_doc_kind
        and (
            (
                signal.scope_kind == "issuer_fingerprint"
                and signal.scope_value == issuer_fingerprint
            )
            or
            (signal.scope_kind == "sender_email" and signal.scope_value == normalized_sender)
            or (
                signal.scope_kind == "sender_domain"
                and sender_domain
                and signal.scope_value == sender_domain
            )
        )
    ]
    if not matches:
        return None, "no_candidate"
    best = sorted(
        matches,
        key=lambda item: (
            0
            if item.scope_kind == "issuer_fingerprint"
            else 1 if item.scope_kind == "sender_email" else 2,
            -item.strength,
            -item.correction_count,
            item.scope_value,
        ),
    )[0]
    if best.correction_count < policy.min_corrections:
        return None, "insufficient_corrections"
    if best.consistency_ratio < policy.min_consistency:
        return None, "inconsistent_corrections"
    if best.strength < policy.min_strength:
        return None, "weak_strength"
    return (
        RuntimeTemplatePromotion(
            signal=best,
            confidence_boost=policy.confidence_boost,
        ),
        None,
    )


def analyze_template_promotion_candidates(
    db_path: Path | str,
    *,
    min_corrections: int = 3,
    min_consistency: float = 0.75,
    min_distinct_senders_for_domain: int = 2,
) -> tuple[TemplatePromotionSignal, ...]:
    path = Path(db_path)
    rows: list[sqlite3.Row]
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                correction.account_id AS account_id,
                correction.email_id AS email_id,
                correction.payload_json AS correction_payload_json,
                (
                    SELECT interpretation.payload_json
                    FROM events_v1 AS interpretation
                    WHERE interpretation.event_type = 'message_interpretation'
                      AND interpretation.email_id = correction.email_id
                    ORDER BY interpretation.id DESC
                    LIMIT 1
                ) AS interpretation_payload_json
            FROM events_v1 AS correction
            WHERE correction.event_type = 'priority_correction_recorded'
            ORDER BY correction.account_id, correction.email_id, correction.id
            """
        ).fetchall()

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        correction_payload = _safe_json_loads(row["correction_payload_json"])
        interpretation_payload = _safe_json_loads(row["interpretation_payload_json"])
        doc_kind = _normalize_text(interpretation_payload.get("doc_kind"))
        template_id = _suggest_template_id(doc_kind)
        sender_email = _normalize_text(interpretation_payload.get("sender_email"))
        if not template_id or not sender_email:
            continue
        identity = normalize_sender_identity(
            sender_email,
            display_name=str(interpretation_payload.get("issuer_label") or ""),
            subject_hint=str(
                interpretation_payload.get("subject_normalized")
                or interpretation_payload.get("subject")
                or ""
            ),
            doc_marker=str(interpretation_payload.get("issuer_tax_id") or ""),
        )
        issuer_fingerprint = build_issuer_fingerprint(identity, doc_kind)
        corrected_priority = (
            str(correction_payload.get("new_priority") or "").strip()
            or str(correction_payload.get("correction") or "").strip()
        )
        if not corrected_priority:
            continue
        account_id = str(row["account_id"] or "").strip()
        email_id = int(row["email_id"] or 0)
        sender_domain = sender_email.split("@", 1)[1] if "@" in sender_email else ""
        entry = {
            "priority": corrected_priority,
            "sender_email": sender_email,
            "email_id": email_id,
            "issuer_fingerprint": issuer_fingerprint,
            "identity_confidence": str(identity.get("confidence") or "").strip().lower(),
        }
        grouped[(account_id, "issuer_fingerprint", issuer_fingerprint, doc_kind)].append(
            entry
        )
        grouped[(account_id, "sender_email", sender_email, doc_kind)].append(entry)
        if sender_domain:
            grouped[(account_id, "sender_domain", sender_domain, doc_kind)].append(entry)

    signals: list[TemplatePromotionSignal] = []
    for (account_id, scope_kind, scope_value, doc_kind), entries in grouped.items():
        correction_count = len(entries)
        if correction_count < max(1, int(min_corrections)):
            continue
        sender_emails = tuple(sorted({str(item["sender_email"]) for item in entries}))
        if (
            scope_kind == "sender_domain"
            and len(sender_emails) < max(1, int(min_distinct_senders_for_domain))
        ):
            continue
        priority_counts = Counter(str(item["priority"]) for item in entries)
        dominant_priority, dominant_count = priority_counts.most_common(1)[0]
        consistency_ratio = dominant_count / correction_count
        if consistency_ratio < float(min_consistency):
            continue
        strength = round(
            consistency_ratio
            * min(1.0, correction_count / max(float(min_corrections), 1.0)),
            4,
        )
        issuer_fingerprints = {
            str(item.get("issuer_fingerprint") or "").strip() for item in entries
        }
        identity_confidences = {
            str(item.get("identity_confidence") or "").strip().lower() for item in entries
        }
        signals.append(
            TemplatePromotionSignal(
                account_id=account_id,
                scope_kind=scope_kind,
                scope_value=scope_value,
                doc_kind=doc_kind,
                template_id=_suggest_template_id(doc_kind) or "",
                issuer_fingerprint=(
                    sorted(issuer_fingerprints)[0] if len(issuer_fingerprints) == 1 else ""
                ),
                identity_confidence=(
                    "strong"
                    if "strong" in identity_confidences
                    else "medium" if "medium" in identity_confidences else "weak"
                ),
                correction_count=correction_count,
                dominant_priority=dominant_priority,
                consistency_ratio=round(consistency_ratio, 4),
                strength=strength,
                sender_emails=sender_emails,
                email_ids=tuple(sorted({int(item["email_id"]) for item in entries})),
            )
        )
    return tuple(
        sorted(
            signals,
            key=lambda item: (
                -item.strength,
                -item.correction_count,
                item.account_id,
                item.scope_kind,
                item.scope_value,
                item.doc_kind,
            ),
        )
    )


__all__ = [
    "RuntimePromotionPolicy",
    "RuntimeTemplatePromotion",
    "TemplatePromotionSignal",
    "analyze_template_promotion_candidates",
    "clear_runtime_template_promotion_cache",
    "find_runtime_template_promotion",
]
