from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


@dataclass(slots=True)
class Commitment:
    commitment_text: str
    deadline_iso: str | None
    status: str
    source: str
    confidence: float


_VERB_PATTERN = re.compile(
    r"\b(вышлю|пришлю|отправлю|созвонимся|встретимся|согласую|уточню|проверю)\b",
    re.IGNORECASE,
)

_DATE_PATTERN = re.compile(
    r"\b(?:до\s*)?(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b",
    re.IGNORECASE,
)

_WEEKDAY_MAP = {
    "понедельник": 0,
    "вторник": 1,
    "среда": 2,
    "среду": 2,
    "четверг": 3,
    "пятница": 4,
    "пятницу": 4,
    "суббота": 5,
    "субботу": 5,
    "воскресенье": 6,
}

_RELATIVE_DAYS = {
    "сегодня": 0,
    "завтра": 1,
    "послезавтра": 2,
}


def _normalize_snippet(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text.strip())
    return cleaned


def extract_deadline_ru(text: str) -> Optional[str]:
    if not text:
        return None

    lowered = text.lower()
    today = date.today()

    for keyword, delta in _RELATIVE_DAYS.items():
        if keyword in lowered:
            return (today + timedelta(days=delta)).isoformat()

    weekday_match = re.search(
        r"\bв\s+(понедельник|вторник|среду|среда|четверг|пятницу|пятница|субботу|суббота|воскресенье)\b",
        lowered,
    )
    if weekday_match:
        weekday_key = weekday_match.group(1)
        target = _WEEKDAY_MAP.get(weekday_key)
        if target is not None:
            delta = (target - today.weekday()) % 7
            if delta == 0:
                delta = 7
            return (today + timedelta(days=delta)).isoformat()

    date_match = _DATE_PATTERN.search(lowered)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        year_raw = date_match.group(3)
        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        else:
            year = today.year
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    return None


def detect_commitments(text: str) -> list[Commitment]:
    if not text:
        return []

    commitments: list[Commitment] = []
    seen: set[str] = set()

    segments = re.split(r"[!?\n\r]+|\.(?!\d)", text)
    for segment in segments:
        if len(commitments) >= 5:
            break
        if not segment.strip():
            continue
        if not _VERB_PATTERN.search(segment):
            continue

        deadline_iso = extract_deadline_ru(segment)
        normalized = _normalize_snippet(segment)
        normalized_key = normalized.lower()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)

        confidence = 0.7
        if deadline_iso or re.search(r"\bдо\b", segment, re.IGNORECASE):
            confidence = 0.9

        commitments.append(
            Commitment(
                commitment_text=normalized,
                deadline_iso=deadline_iso,
                status="pending",
                source="heuristic",
                confidence=confidence,
            )
        )

    return commitments


__all__ = ["Commitment", "detect_commitments", "extract_deadline_ru"]
