from __future__ import annotations

import html
import re
from typing import Iterable, Optional


def normalize_text(text: str) -> str:
    """Normalize text for deterministic extraction."""

    if not isinstance(text, str):
        return ""

    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = re.sub(
        r"<(head|script|style)[^>]*>.*?</\1>",
        " ",
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )
    cleaned = re.sub(r"<!--.*?-->", " ", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = html.unescape(cleaned)

    cleaned = re.sub(r"[_]{3,}", " ", cleaned)
    cleaned = re.sub(r"[\-=]{3,}", " ", cleaned)
    cleaned = cleaned.replace("|", " | ")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def pick_email_body_fact(body_text: str) -> Optional[str]:
    """Pick a single factual snippet from email body."""

    normalized = normalize_text(body_text)
    if not normalized:
        return None

    filtered_lines: list[str] = []
    for raw_line in normalized.split("\n"):
        line = raw_line.strip()
        lowered = line.lower()
        if not line:
            continue
        if lowered.startswith(
            (
                "from:",
                "sent:",
                "to:",
                "subject:",
                "cc:",
                "bcc:",
                "от:",
                "кому:",
                "тема:",
                "дата:",
            )
        ):
            continue
        if lowered.startswith((">", "fw:", "fwd:", "forwarded message")):
            continue
        if lowered.startswith(("добрый день", "здравствуйте", "привет", "hello", "hi")):
            continue
        filtered_lines.append(line)

    if not filtered_lines:
        return None

    text = " ".join(filtered_lines)
    sentences = re.split(r"(?<=[.!?])\s+|\n", text)

    greetings = ("добрый день", "добрый вечер", "здравствуйте", "привет", "hello", "hi")
    signatures = ("с уважением", "best regards", "kind regards")
    keywords = (
        "счёт",
        "оплата",
        "долг",
        "задолженность",
        "договор",
        "прайс",
        "срок",
        "до",
    )
    date_pattern = re.compile(
        r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b|"
        r"\b(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+\d{4}\b",
        re.IGNORECASE,
    )

    for sentence in sentences:
        trimmed = sentence.strip()
        if not trimmed:
            continue
        lowered = trimmed.lower()
        if any(lowered.startswith(greet) for greet in greetings):
            continue
        if any(lowered.startswith(sig) for sig in signatures):
            continue

        has_number = bool(re.search(r"\d", trimmed) or date_pattern.search(trimmed))
        has_keyword = any(word in lowered for word in keywords)

        if has_number or has_keyword:
            return _trim_words(trimmed, 16)

    return None


def pick_attachment_fact(text: str, filename: str, doc_type: str) -> Optional[str]:
    """Deterministically extract a factual snippet from attachment text."""

    normalized = normalize_text(text)
    if not normalized:
        return None

    segments = _collect_segments(normalized)

    money_pattern = re.compile(
        r"₽|\bруб\.?\b|\bр\.\b|USD|\$|EUR|€|сумма|итого|к оплате", re.IGNORECASE
    )
    for segment in segments:
        if money_pattern.search(segment):
            return _safe_clause(segment)

    date_pattern = re.compile(
        r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b|\b(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+\d{4}\b|\bдо\s+\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?",
        re.IGNORECASE,
    )
    for segment in segments:
        if date_pattern.search(segment):
            return _safe_clause(segment)

    contract_headers = (
        "соглашение",
        "договор",
        "стороны",
        "поставщик",
        "покупатель",
        "предмет договора",
    )
    contract_candidates = [
        seg
        for seg in segments
        if any(header.lower() in seg.lower() for header in contract_headers)
    ]
    if contract_candidates:
        rich_candidates = [seg for seg in contract_candidates if len(seg.split()) > 1]
        chosen = min(rich_candidates or contract_candidates, key=len)
        return _safe_clause(chosen)

    header_tokens = _excel_headers(segments)
    if header_tokens:
        return _trim_words(", ".join(header_tokens), 16)

    repeated = _repeated_word(normalized)
    if repeated:
        return _trim_words(repeated, 16)

    dense = _dense_line(segments)
    if dense:
        return _safe_clause(dense)

    return None


def _collect_segments(text: str) -> list[str]:
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    segments: list[str] = []
    for line in lines:
        parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", line) if p.strip()]
        if parts:
            segments.extend(parts)
        else:
            segments.append(line)
    return segments or [text.strip()]


def _safe_clause(segment: str) -> Optional[str]:
    trimmed = _trim_words(segment, 16)
    if not trimmed:
        return None
    if "____" in trimmed:
        return None
    if re.fullmatch(r"[\W_]+", trimmed):
        return None
    return trimmed


def _trim_words(text: str, limit: int) -> str:
    words = text.split()
    return " ".join(words[:limit]).strip()


def _excel_headers(segments: Iterable[str]) -> list[str]:
    header_words = ("код", "наименование", "цена", "количество", "итог")
    for segment in list(segments)[:5]:
        tokens = [tok for tok in re.split(r"[|;,]", segment) if tok.strip()]
        cleaned_tokens = [tok.strip() for tok in tokens if tok.strip()]
        lowered = [tok.lower() for tok in cleaned_tokens]
        if sum(1 for word in header_words if word in lowered) >= 2:
            return cleaned_tokens
    return []


def _repeated_word(text: str) -> Optional[str]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё\-]{4,}", text)
    counts = {}
    for word in words:
        lowered = word.lower()
        counts[lowered] = counts.get(lowered, 0) + 1
    for word, count in counts.items():
        if count >= 3:
            return word
    return None


def _dense_line(segments: Iterable[str]) -> Optional[str]:
    best: tuple[int, str] | None = None
    for segment in segments:
        length = len(segment)
        if length < 30 or length > 120:
            continue
        tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]{4,}", segment)
        if not tokens:
            continue
        digits = sum(ch.isdigit() for ch in segment)
        if digits > length * 0.6:
            continue
        score = len(tokens)
        if (
            best is None
            or score > best[0]
            or (score == best[0] and length < len(best[1]))
        ):
            best = (score, segment)
    return best[1] if best else None


__all__ = ["normalize_text", "pick_email_body_fact", "pick_attachment_fact"]
