"""Lightweight regex-based action extractor (Guaranteed Mode)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ActionFacts:
    action: Optional[str]
    amount: Optional[str]
    date: Optional[str]
    doc_number: Optional[str]
    urgency: Optional[str]
    confidence: float


_AMOUNT_RE = re.compile(r"(?P<amount>\d+[\d\s]*[\.,]?\d*)\s*(?P<currency>₽|руб|рублей|usd|eur|€|\$)?", re.IGNORECASE)
_DATE_RE = re.compile(r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})")
_DOC_RE = re.compile(r"(?:№|no\.?|num(?:ber)?)[\s:]*([A-Za-z0-9_-]{3,})", re.IGNORECASE)
_ACTION_RE = re.compile(r"(оплатить|оплата|pay|payment|approve|утвердить|подписать)", re.IGNORECASE)
_URGENT_RE = re.compile(r"(срочно|urgent|asap|немедленно)", re.IGNORECASE)


def _confidence_score(found: int, total_checks: int = 5) -> float:
    base = 0.1 if found else 0.0
    scaled = base + (found / max(total_checks, 1))
    return min(1.0, round(scaled, 2))


def analyze_action(text: str) -> ActionFacts:
    """Extract primitive action hints using regex only (<10ms).

    The function intentionally avoids any ML/LLM usage per Constitution
    and returns a conservative confidence score.
    """

    amount_match = _AMOUNT_RE.search(text)
    date_match = _DATE_RE.search(text)
    doc_match = _DOC_RE.search(text)
    action_match = _ACTION_RE.search(text)
    urgency_match = _URGENT_RE.search(text)

    found_fields = sum(
        1
        for match in (amount_match, date_match, doc_match, action_match, urgency_match)
        if match is not None
    )
    confidence = _confidence_score(found_fields)

    amount = None
    if amount_match:
        amount = amount_match.group("amount")
        currency = amount_match.group("currency") or ""
        amount = f"{amount}{currency}".strip()

    date = date_match.group(1) if date_match else None
    doc_number = doc_match.group(1) if doc_match else None
    action = action_match.group(1).lower() if action_match else None
    urgency = urgency_match.group(1).lower() if urgency_match else None

    return ActionFacts(
        action=action,
        amount=amount,
        date=date,
        doc_number=doc_number,
        urgency=urgency,
        confidence=confidence,
    )


__all__ = ["ActionFacts", "analyze_action"]
