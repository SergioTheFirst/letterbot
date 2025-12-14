from __future__ import annotations

import re
from typing import Any


BINARY_MARKERS = (
    "ihdr",
    "idat",
    "pk",
    "content_types",
    "=?koi8",
    "=?utf-",
    "=?windows-",
    "base64",
    "image/png",
    "zip",
)

BASE64_RUN = re.compile(r"[A-Za-z0-9+/]{40,}={0,2}")
ENCODED_HEADER = re.compile(r"=\?.+\?[bqBQ]\?.+\?=")


def _to_str(text: Any) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        try:
            return text.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    try:
        return str(text)
    except Exception:
        return ""


def is_binaryish(text: str) -> bool:
    data = _to_str(text)
    if not data:
        return False

    if "\x00" in data:
        return True

    lowered = data.lower()
    if any(marker in lowered for marker in BINARY_MARKERS):
        return True

    if BASE64_RUN.search(data) or ENCODED_HEADER.search(data):
        return True

    non_printable = sum(1 for ch in data if not (ch.isprintable() or ch in "\n\r\t"))
    if non_printable > 5 and non_printable > len(data) * 0.02:
        return True

    return False


def sanitize_text(text: Any, max_len: int = 8000) -> str:
    try:
        normalized = _to_str(text)
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.replace("\x00", "")
    except Exception:
        return ""

    safe_lines: list[str] = []
    for raw_line in normalized.split("\n"):
        line = raw_line.strip("\ufeff")
        if is_binaryish(line):
            continue
        safe_lines.append(line)

    collapsed: list[str] = []
    blank = False
    for line in safe_lines:
        compact = re.sub(r"\s+", " ", line).strip()
        if not compact:
            if blank:
                continue
            collapsed.append("")
            blank = True
            continue
        collapsed.append(compact)
        blank = False

    cleaned = "\n".join(collapsed).strip()
    if len(cleaned) > max_len:
        return cleaned[: max_len - 3] + "..."
    return cleaned


__all__ = ["sanitize_text", "is_binaryish"]
