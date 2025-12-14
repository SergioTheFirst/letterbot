from __future__ import annotations

import html
import re
from typing import Any

FORWARD_MARKERS = (
    "from:",
    "sent:",
    "to:",
    "subject:",
    "от:",
    "кому:",
    "тема:",
    "отправлено:",
    "-----original message-----",
    "----- forwarded message -----",
)

SIGNATURE_MARKERS = (
    "с уважением,",
    "regards,",
)


def _to_str(text: Any) -> str:
    if text is None:
        return ""
    try:
        return str(text)
    except Exception:
        return ""


def _is_forward_start(line: str) -> bool:
    lower = line.strip().lower()
    if lower.startswith("--"):
        return True
    return any(lower.startswith(marker) for marker in FORWARD_MARKERS)


def _is_signature_start(line: str) -> bool:
    lower = line.strip().lower()
    if lower.startswith("--"):
        return True
    return any(lower.startswith(marker) for marker in SIGNATURE_MARKERS)


def _looks_like_html(text: str) -> bool:
    lowered = text.lower()
    if any(tag in lowered for tag in ("<html", "<body", "<style", "<table", "<!doctype")):
        return True
    return bool(re.search(r"<[^>]+>", text))


def _strip_html(text: str) -> str:
    working = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    working = re.sub(r"<!--.*?-->", " ", working, flags=re.DOTALL)
    working = re.sub(r"<(br|p|div|tr|td|th|li|ul|ol|h[1-6])[^>]*>", "\n", working, flags=re.IGNORECASE)
    working = re.sub(r"</(p|div|tr|td|th|li|ul|ol|h[1-6])>", "\n", working, flags=re.IGNORECASE)
    working = re.sub(r"<[^>]+>", " ", working)
    working = html.unescape(working)
    working = re.sub(r"[ \t]+", " ", working)
    working = re.sub(r"(\n\s*){2,}", "\n\n", working)
    return working.strip()


def clean_email_body(text: Any) -> str:
    try:
        normalized = _to_str(text).replace("\r\n", "\n").replace("\r", "\n")
    except Exception:
        return ""

    if _looks_like_html(normalized):
        normalized = _strip_html(normalized)
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")

    lines = normalized.split("\n")
    cleaned: list[str] = []

    for line in lines:
        if _is_forward_start(line):
            break
        if _is_signature_start(line):
            break
        cleaned.append(line)

    collapsed: list[str] = []
    blank = False
    for line in cleaned:
        stripped = line.strip()
        if not stripped:
            if blank:
                continue
            collapsed.append("")
            blank = True
            continue
        collapsed.append(stripped)
        blank = False

    result = "\n".join(collapsed).strip()
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


__all__ = ["clean_email_body"]
