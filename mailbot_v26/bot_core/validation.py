"""Validation helpers for post-LLM cleaning."""
from __future__ import annotations

import re
from typing import List


def drop_none_tokens(facts_line: str) -> str:
    """Remove NONE tokens and trim separators."""
    if not facts_line:
        return ""
    tokens = [token.strip() for token in facts_line.split("|") if token.strip()]
    filtered = [t for t in tokens if t.lower() != "none"]
    return " | ".join(filtered).strip()


def clean_none(text: str) -> str:
    """Remove any segment containing NONE (case-insensitive)."""
    if not text:
        return ""
    if text.strip().lower() == "none":
        return ""
    segments = [segment.strip() for segment in text.split("|") if segment.strip()]
    filtered = [segment for segment in segments if "none" not in segment.lower()]
    return " | ".join(filtered)


def _extract_numbers(text: str) -> List[str]:
    return [match.strip() for match in re.findall(r"\d+[\d\s]*[\.,]?\d*", text)]


def validate_numbers(summary: str, original: str) -> bool:
    """Ensure every number in the summary is present in the original text."""
    numbers = _extract_numbers(summary)
    lowered_original = original or ""
    for number in numbers:
        if number not in lowered_original:
            return False
    return True


def _normalize_date(value: str) -> str:
    return re.sub(r"[/-]", ".", value)


STOP_WORDS = {
    "на",
    "до",
    "и",
    "в",
    "к",
    "по",
    "за",
    "с",
    "от",
    "просим",
}


def validate_dates(summary: str, original: str) -> bool:
    date_re = re.compile(r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}")
    summary_dates = [
        _normalize_date(match) for match in date_re.findall(summary or "")
    ]
    if not summary_dates:
        return True

    original_dates = {
        _normalize_date(match) for match in date_re.findall(original or "")
    }
    return all(date in original_dates for date in summary_dates)


def jaccard_similarity(text1: str, text2: str) -> float:
    def _tokenize(text: str) -> set[str]:
        raw_tokens = re.findall(r"[\w\.]+", (text or "").lower())
        tokens: set[str] = set()
        for raw in raw_tokens:
            token = raw.replace("ё", "е").strip("._")
            while len(token) > 3 and token[-1] in "аеёиоуыэюяй":
                token = token[:-1]
            if not token or token in STOP_WORDS:
                continue
            tokens.add(token)
        return tokens

    tokens1 = _tokenize(text1)
    tokens2 = _tokenize(text2)
    if not tokens1 or not tokens2:
        return 0.0
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    return len(intersection) / len(union) if union else 0.0


def validate_summary(summary: str, original: str, min_similarity: float = 0.35) -> str | None:
    cleaned = clean_none(summary)
    if not cleaned:
        return None
    if not validate_numbers(cleaned, original):
        return None
    if not validate_dates(cleaned, original):
        return None
    similarity = jaccard_similarity(cleaned, original)
    if similarity < min_similarity:
        return None
    return cleaned


def ensure_length(text: str, limit: int = 200) -> str:
    """Trim text to the Constitution limit."""
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip(" |")


def is_confident_score(score: float) -> bool:
    return score >= 0.7


def _self_test():
    original = "Просим оплатить счёт №123 на сумму 150000 рублей до 20.12.2024"
    good = "СУММА: 150000 | СРОК: 20.12.2024 | ДОКУМЕНТ: №123"
    bad = "СУММА: 999000 | СРОК: 01.01.2099 | ДОКУМЕНТ: №999"
    mix = "СУММА: 150000 | СРОК: NONE | ДОКУМЕНТ: №123 | ДЕЙСТВИЕ: оплатить"

    assert clean_none(mix) == "СУММА: 150000 | ДОКУМЕНТ: №123 | ДЕЙСТВИЕ: оплатить"
    assert validate_numbers(good, original) is True
    assert validate_numbers(bad, original) is False
    assert validate_dates(good, original) is True
    assert validate_dates(bad, original) is False

    vs = validate_summary(good, original)
    assert vs is not None

    vs_bad = validate_summary(bad, original)
    assert vs_bad is None

    print("OK: validation self-test passed")


__all__ = [
    "clean_none",
    "drop_none_tokens",
    "ensure_length",
    "is_confident_score",
    "jaccard_similarity",
    "validate_dates",
    "validate_numbers",
    "validate_summary",
]


if __name__ == "__main__":
    _self_test()
