from __future__ import annotations

import re

_MOJIBAKE_MARKERS = (
    "\u0432\u0402",
    "\u0440\u045f",
    "\u0420\u045f",
    "\u00D0",
    "\u00D1",
    "\u00F0\u0178",
    "\u00F0",
    "\u00E2\u20AC\u201D",
    "\u00E2\u20AC\u201C",
    "\u00E2\u20AC\u00A6",
    "\u00E2\u20AC\u00A2",
    "\u00C3",
    "\u00C2",
    "\uFFFD",
)

_MOJIBAKE_PAIR_RE = re.compile(r"(?:[\u0420\u0421\u0440\u0441][\u0080-\u00FF\u0400-\u04FF]){2,}")

_PUNCTUATION_REPLACEMENTS = {
    "\u0432\u0402\u201D": "\u2014",
    "\u0432\u0402\u201C": "\u2013",
    "\u0432\u0402\u00A6": "\u2026",
    "\u0432\u0402\u045E": "\u2022",
    "\u00E2\u20AC\u201D": "\u2014",
    "\u00E2\u20AC\u201C": "\u2013",
    "\u00E2\u20AC\u00A6": "\u2026",
    "\u00E2\u20AC\u00A2": "\u2022",
    "\u00E2\u201E\u2016": "\u2116",
    "\u0420\u0455\u0421\u201A": "\u043E\u0442",
    "\u00D0\u00BE\u00D1\u201A": "\u043E\u0442",
    "\u0412\u00B7": "\u00B7",
}


def _contains_mojibake(text: str) -> bool:
    if any(marker in text for marker in _MOJIBAKE_MARKERS):
        return True
    return _MOJIBAKE_PAIR_RE.search(text) is not None


def _repair_with_encoding(source: str, encoding: str) -> str:
    protected_parts: list[str] = []
    placeholders: dict[str, str] = {}
    protected_index = 0
    for char in source:
        try:
            char.encode(encoding)
            protected_parts.append(char)
        except UnicodeEncodeError:
            token = f"__mb_{protected_index}__"
            placeholders[token] = char
            protected_parts.append(token)
            protected_index += 1
    protected = "".join(protected_parts)

    repaired = protected
    try:
        repaired = protected.encode(encoding).decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        repaired = protected

    for token, char in placeholders.items():
        repaired = repaired.replace(token, char)
    return repaired


def _mojibake_score(text: str) -> int:
    score = 0
    for marker in _MOJIBAKE_MARKERS:
        score += text.count(marker) * 4
    for bad in _PUNCTUATION_REPLACEMENTS:
        score += text.count(bad) * 2
    for match in _MOJIBAKE_PAIR_RE.finditer(text):
        score += len(match.group(0))
    return score


def _normalize_chunk(source: str) -> str:
    repaired = source
    for _ in range(3):
        if not _contains_mojibake(repaired):
            break
        candidates = [
            repaired,
            _repair_with_encoding(repaired, "cp1251"),
            _repair_with_encoding(repaired, "latin-1"),
            _repair_with_encoding(repaired, "cp1252"),
        ]
        best = min(candidates, key=_mojibake_score)
        if best == repaired:
            break
        repaired = best
    return repaired


def normalize_mojibake_text(text: str) -> str:
    source = str(text or "")
    if not source:
        return ""

    repaired = _normalize_chunk(source)
    if _contains_mojibake(repaired) and ("\n" in repaired or "\r" in repaired):
        parts = re.split(r"(\r\n|\n|\r)", repaired)
        repaired = "".join(
            part if part in {"\r\n", "\n", "\r"} else _normalize_chunk(part)
            for part in parts
        )

    for bad, good in _PUNCTUATION_REPLACEMENTS.items():
        repaired = repaired.replace(bad, good)
    return repaired


__all__ = ["normalize_mojibake_text"]
