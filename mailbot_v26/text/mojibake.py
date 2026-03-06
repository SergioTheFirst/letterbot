from __future__ import annotations


def normalize_mojibake_text(text: str) -> str:
    source = str(text or "")
    if not source:
        return ""
    if not any(marker in source for marker in ("Р", "С", "вЂ", "рџ")):
        return source

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

    replacements = {
        "вЂ”": "—",
        "вЂ“": "–",
        "вЂ¦": "…",
        "вЂў": "•",
    }
    for bad, good in replacements.items():
        repaired = repaired.replace(bad, good)
    return repaired


__all__ = ["normalize_mojibake_text"]
