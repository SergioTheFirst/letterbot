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


def _non_encodable_run_re(encoding: str) -> re.Pattern[str]:
    """Match runs of characters that single-byte ``encoding`` cannot encode.

    In a charmap codec a character is encodable exactly when some byte decodes
    to it (locked by test_non_encodable_classification_matches_codec).
    """
    encodable: list[str] = []
    for byte in range(256):
        try:
            encodable.append(bytes([byte]).decode(encoding))
        except UnicodeDecodeError:
            continue  # byte undefined in this charmap
    return re.compile("([^" + "".join(re.escape(char) for char in encodable) + "]+)")


_NON_ENCODABLE_RUN_RES = {
    encoding: _non_encodable_run_re(encoding)
    for encoding in ("cp1251", "latin-1", "cp1252")
}
# A literal "mb_<digit>" in the source can form a "__mb_N__" look-alike that
# overlaps the placeholders of _repair_with_placeholders. The original
# algorithm then also replaced the look-alike; that output is kept as is.
_PLACEHOLDER_LOOKALIKE_RE = re.compile(r"mb_[0-9]")
_PLACEHOLDER_RE = re.compile(r"(?=__mb_(0|[1-9][0-9]*)__)")


def _restore_placeholders(text: str, protected_chars: list[str]) -> str:
    """Put protected characters back in one pass.

    Returns what the original loop returned, in linear time instead of one
    whole-text ``str.replace`` per placeholder::

        for index, char in enumerate(protected_chars):
            text = text.replace(f"__mb_{index}__", char)

    When a look-alike overlaps a placeholder the lower index wins, and matches
    of one index are taken left to right without overlap, exactly as in the
    loop. The replacement characters are never ASCII (every ASCII character is
    encodable), so a replacement can only destroy overlapping matches of later
    indexes, never create new ones.
    """
    count = len(protected_chars)
    max_digits = len(str(count))
    candidates: list[tuple[int, int, int]] = []
    for match in _PLACEHOLDER_RE.finditer(text):
        digits = match.group(1)
        if len(digits) > max_digits:
            continue
        index = int(digits)
        if index >= count:
            continue
        start = match.start()
        end = start + len("__mb_") + len(digits) + len("__")
        candidates.append((index, start, end))
    candidates.sort()

    taken = bytearray(len(text))
    chosen: list[tuple[int, int, str]] = []
    for index, start, end in candidates:
        if any(taken[start:end]):
            continue
        taken[start:end] = b"\x01" * (end - start)
        chosen.append((start, end, protected_chars[index]))
    chosen.sort()

    pieces: list[str] = []
    cursor = 0
    for start, end, char in chosen:
        pieces.append(text[cursor:start])
        pieces.append(char)
        cursor = end
    pieces.append(text[cursor:])
    return "".join(pieces)


def _repair_with_placeholders(
    source: str, encode_encoding: str, decode_encoding: str
) -> str:
    protected_parts: list[str] = []
    protected_chars: list[str] = []
    for char in source:
        try:
            char.encode(encode_encoding)
            protected_parts.append(char)
        except UnicodeEncodeError:
            protected_parts.append(f"__mb_{len(protected_chars)}__")
            protected_chars.append(char)
    protected = "".join(protected_parts)

    try:
        repaired = protected.encode(encode_encoding).decode(decode_encoding)
    except (UnicodeEncodeError, UnicodeDecodeError):
        repaired = protected
    return _restore_placeholders(repaired, protected_chars)


def _repair_with_encoding(
    source: str, encode_encoding: str, decode_encoding: str
) -> str:
    """Re-decode ``source`` as ``decode_encoding`` bytes, keeping the characters
    ``encode_encoding`` cannot encode; when the bytes do not decode, the text
    is not repaired. Output is identical to the original placeholder loop."""
    non_encodable = _NON_ENCODABLE_RUN_RES.get(encode_encoding)
    if non_encodable is None or _PLACEHOLDER_LOOKALIKE_RE.search(source):
        return _repair_with_placeholders(source, encode_encoding, decode_encoding)
    # Without a look-alike every placeholder occurs exactly once, so the
    # placeholder result equals re-decoding each encodable run on its own
    # (placeholders are ASCII: no multi-byte sequence can span one) and keeping
    # the non-encodable runs as they are.
    parts = non_encodable.split(source)
    try:
        repaired = [
            part if index % 2 else part.encode(encode_encoding).decode(decode_encoding)
            for index, part in enumerate(parts)
        ]
    except (UnicodeEncodeError, UnicodeDecodeError):
        return source
    return "".join(repaired)


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
