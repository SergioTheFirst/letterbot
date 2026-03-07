from __future__ import annotations


def _limit_words(text: str, limit: int) -> str:
    words = text.split()
    if len(words) <= limit:
        return " ".join(words)
    return " ".join(words[:limit]).strip()


def compress_attachment_fact(snippet: str, doc_type: str | None = None) -> str:
    if not snippet:
        return ""
    compressed = _limit_words(snippet, 14)
    return compressed or snippet


def compress_body_fact(body_snippet: str) -> str:
    if not body_snippet:
        return ""
    compressed = _limit_words(body_snippet, 16)
    return compressed or body_snippet


__all__ = ["compress_attachment_fact", "compress_body_fact"]
