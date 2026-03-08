from __future__ import annotations

import re

_MOJIBAKE_MARKERS = (
    "\u0432\u0402",
    "\u0440\u045f",
    "\u0420\u045f",
    "\u00d0",
    "\u00d1",
    "\u00f0\u0178",
    "\u00f0",
    "\u00e2\u20ac\u201d",
    "\u00e2\u20ac\u201c",
    "\u00e2\u20ac\u00a6",
    "\u00e2\u20ac\u00a2",
    "\u00c3",
    "\u00c2",
    "\ufffd",
)

_MOJIBAKE_PAIR_RE = re.compile(
    r"(?:[\u0420\u0421\u0440\u0441][\u0080-\u00FF\u0400-\u04FF]){2,}"
)
_LATIN1_GARBAGE_RE = re.compile(r"[\u00C0-\u00FF]{3,}")

_PUNCTUATION_REPLACEMENTS = {
    "\u0432\u0402\u201d": "\u2014",
    "\u0432\u0402\u201c": "\u2013",
    "\u0432\u0402\u00a6": "\u2026",
    "\u0432\u0402\u045e": "\u2022",
    "\u00e2\u20ac\u201d": "\u2014",
    "\u00e2\u20ac\u201c": "\u2013",
    "\u00e2\u20ac\u00a6": "\u2026",
    "\u00e2\u20ac\u00a2": "\u2022",
    "\u00e2\u201e\u2016": "\u2116",
    "\u0420\u0455\u0421\u201a": "\u043e\u0442",
    "\u00d0\u00be\u00d1\u201a": "\u043e\u0442",
    "\u0412\u00b7": "\u00b7",
    # Common mojibake sequences for priority circles
    "\u0440\u045f\u201d\u0491": "🔴",
    "\u0440\u045f\u201f\u0160": "🟡",
    "\u0440\u045f\u201d\u00b5": "🔵",
    "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0422\u2018": "🔴",
    "\u0441\u0452\u0441\u045f\u0421\u045f\u045f\u0420\u040b": "🟡",
    "\u0441\u0452\u0441\u045f\u0432\u0402\u045c\u0412\u00b5": "🔵",
    "\u00f0\u0178\u201d\u00b4": "🔴",
    "\u00f0\u0178\u0178\u00a1": "🟡",
    "\u00f0\u0178\u201d\u00b5": "🔵",
}


def _contains_mojibake(text: str) -> bool:
    if any(marker in text for marker in _MOJIBAKE_MARKERS):
        return True
    if _LATIN1_GARBAGE_RE.search(text):
        return True
    return _MOJIBAKE_PAIR_RE.search(text) is not None


def _repair_with_encoding(
    source: str, encode_encoding: str, decode_encoding: str
) -> str:
    protected_parts: list[str] = []
    placeholders: dict[str, str] = {}
    protected_index = 0
    for char in source:
        try:
            char.encode(encode_encoding)
            protected_parts.append(char)
        except UnicodeEncodeError:
            token = f"__mb_{protected_index}__"
            placeholders[token] = char
            protected_parts.append(token)
            protected_index += 1
    protected = "".join(protected_parts)

    repaired = protected
    try:
        repaired = protected.encode(encode_encoding).decode(decode_encoding)
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
    for match in _LATIN1_GARBAGE_RE.finditer(text):
        score += len(match.group(0)) * 2
    return score


def _normalize_chunk(source: str) -> str:
    repaired = source
    for _ in range(3):
        if not _contains_mojibake(repaired):
            break
        candidates = [
            repaired,
            _repair_with_encoding(repaired, "cp1251", "utf-8"),
            _repair_with_encoding(repaired, "latin-1", "utf-8"),
            _repair_with_encoding(repaired, "cp1252", "utf-8"),
            _repair_with_encoding(repaired, "latin-1", "cp1251"),
            _repair_with_encoding(repaired, "cp1252", "cp1251"),
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
