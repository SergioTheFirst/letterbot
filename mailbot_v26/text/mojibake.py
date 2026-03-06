from __future__ import annotations


_MOJIBAKE_MARKERS = ("Р", "С", "вЂ", "рџ", "·")


def _repair_cp1251_utf8_once(source: str) -> str:
    protected_parts: list[str] = []
    placeholders: dict[str, str] = {}
    protected_index = 0
    for char in source:
        try:
            char.encode("cp1251")
            protected_parts.append(char)
        except UnicodeEncodeError:
            token = f"__mb_{protected_index}__"
            placeholders[token] = char
            protected_parts.append(token)
            protected_index += 1
    protected = "".join(protected_parts)

    repaired = protected
    try:
        repaired = protected.encode("cp1251").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        repaired = protected

    for token, char in placeholders.items():
        repaired = repaired.replace(token, char)
    return repaired


def normalize_mojibake_text(text: str) -> str:
    source = str(text or "")
    if not source:
        return ""
    if not any(marker in source for marker in _MOJIBAKE_MARKERS):
        return source

    repaired = source
    for _ in range(3):
        if not any(marker in repaired for marker in _MOJIBAKE_MARKERS):
            break
        candidate = _repair_cp1251_utf8_once(repaired)
        if candidate == repaired:
            break
        repaired = candidate

    replacements = {
        "—": "—",
        "–": "–",
        "…": "…",
        "•": "•",
        "·": "·",
    }
    for bad, good in replacements.items():
        repaired = repaired.replace(bad, good)
    return repaired


__all__ = ["normalize_mojibake_text"]
