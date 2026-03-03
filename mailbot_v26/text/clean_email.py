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

DISCLAIMER_MARKERS = (
    "внешняя почта:",
    "external email:",
    "this email was sent from outside",
    "caution: external email",
    "this message is from an external sender",
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


def _is_disclaimer_start(line: str) -> bool:
    lowered = line.strip().lower()
    if not lowered:
        return False
    return any(lowered.startswith(marker) for marker in DISCLAIMER_MARKERS)


def _is_disclaimer_prefix(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return True
    if lowered.endswith(("-", "—", "|", "•")):
        return True
    return any(
        marker in lowered
        for marker in ("re:", "fw:", "fwd:", "subject:", "тема:", "from:", "от:")
    )


def _strip_inline_disclaimer(line: str) -> tuple[str, bool]:
    lowered = line.lower()
    best_index: int | None = None
    for marker in DISCLAIMER_MARKERS:
        index = lowered.find(marker)
        if index < 0:
            continue
        if best_index is None or index < best_index:
            best_index = index
    if best_index is None:
        return line, False
    if not _is_disclaimer_prefix(line[:best_index]):
        return line, False
    return line[:best_index].rstrip(), True


def _looks_like_html(text: str) -> bool:
    lowered = text.lower()
    if any(tag in lowered for tag in ("<html", "<body", "<style", "<table", "<!doctype", "<head")):
        return True
    return bool(re.search(r"<[^>]+>", text))


def _strip_html(text: str) -> str:
    working = re.sub(r"<(head|script|style)[^>]*>.*?</\1>", " ", text, flags=re.IGNORECASE | re.DOTALL)
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
        if _is_disclaimer_start(line):
            break
        stripped_line, had_inline_disclaimer = _strip_inline_disclaimer(line)
        if stripped_line:
            cleaned.append(stripped_line)
        if had_inline_disclaimer:
            break

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
