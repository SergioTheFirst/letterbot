from __future__ import annotations

import html
import re
from typing import Any

FORWARD_MARKERS = (
    "from:",
    "sent:",
    "to:",
    "subject:",
    "\u043e\u0442:",
    "\u043a\u043e\u043c\u0443:",
    "\u0442\u0435\u043c\u0430:",
    "\u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e:",
    "----original message----",
    "-----original message-----",
    "----- forwarded message -----",
)

FORWARD_STOP_PHRASES = (
    "from:",
    "forwarded message",
    "----original message----",
    "-----original message-----",
)

SIGNATURE_MARKERS = (
    "\u0441 \u0443\u0432\u0430\u0436\u0435\u043d\u0438\u0435\u043c,",
    "regards,",
)

SEGMENT_FORWARD_MARKERS = (
    "forwarded message",
    "----original message----",
    "-----original message-----",
    "from:",
    "sent:",
)

SEGMENT_SIGNATURE_MARKERS = (
    "best regards",
    "kind regards",
    "\u0441 \u0443\u0432\u0430\u0436\u0435\u043d\u0438\u0435\u043c",
)

DISCLAIMER_MARKERS = (
    "\u0432\u043d\u0435\u0448\u043d\u044f\u044f \u043f\u043e\u0447\u0442\u0430:",
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
    lowered = line.strip().lower()
    if lowered.startswith("--"):
        return True
    compact = re.sub(r"\s+", " ", lowered)
    if any(phrase in compact for phrase in FORWARD_STOP_PHRASES):
        return True
    return any(compact.startswith(marker) for marker in FORWARD_MARKERS)


def _is_signature_start(line: str) -> bool:
    lowered = line.strip().lower()
    if lowered.startswith("--"):
        return True
    return any(lowered.startswith(marker) for marker in SIGNATURE_MARKERS)


def _is_disclaimer_start(line: str) -> bool:
    lowered = line.strip().lower()
    if not lowered:
        return False
    return any(lowered.startswith(marker) for marker in DISCLAIMER_MARKERS)


def _is_disclaimer_prefix(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return True
    if lowered.endswith(("-", "\u2014", "|", "\u2022")):
        return True
    return any(
        marker in lowered
        for marker in (
            "re:",
            "fw:",
            "fwd:",
            "subject:",
            "\u0442\u0435\u043c\u0430:",
            "from:",
            "\u043e\u0442:",
        )
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


def _collapse_lines(lines: list[str]) -> str:
    collapsed: list[str] = []
    blank = False
    for line in lines:
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
    return re.sub(r"\n{3,}", "\n\n", result)


def _is_segment_forward_start(line: str) -> bool:
    compact = re.sub(r"\s+", " ", line.strip().lower())
    if not compact:
        return False
    if any(marker in compact for marker in SEGMENT_FORWARD_MARKERS[:3]):
        return True
    return any(compact.startswith(marker) for marker in SEGMENT_FORWARD_MARKERS[3:])


def _is_segment_signature_start(line: str) -> bool:
    stripped = line.strip()
    lowered = stripped.lower()
    if stripped.startswith("--"):
        return True
    return any(lowered.startswith(marker) for marker in SEGMENT_SIGNATURE_MARKERS)


def _is_quoted_line(line: str) -> bool:
    return line.lstrip().startswith(">")


def segment_email_body(text: Any) -> dict[str, str]:
    normalized = _to_str(text).replace("\r\n", "\n").replace("\r", "\n")
    if _looks_like_html(normalized):
        normalized = _strip_html(normalized)
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")

    main_lines: list[str] = []
    forwarded_lines: list[str] = []
    quoted_lines: list[str] = []
    signature_lines: list[str] = []
    disclaimer_lines: list[str] = []
    zone = "main"

    for line in normalized.split("\n"):
        if zone == "main":
            if _is_segment_forward_start(line):
                zone = "forwarded"
            elif _is_quoted_line(line):
                zone = "quoted"
            elif _is_segment_signature_start(line):
                zone = "signature"
            elif _is_disclaimer_start(line):
                zone = "disclaimer"
        elif zone == "quoted":
            if _is_segment_forward_start(line):
                zone = "forwarded"
            elif _is_segment_signature_start(line):
                zone = "signature"
            elif _is_disclaimer_start(line):
                zone = "disclaimer"
        elif zone == "signature":
            if _is_segment_forward_start(line):
                zone = "forwarded"
            elif _is_disclaimer_start(line):
                zone = "disclaimer"

        if zone == "forwarded":
            forwarded_lines.append(line)
        elif zone == "quoted":
            quoted_lines.append(line)
        elif zone == "signature":
            signature_lines.append(line)
        elif zone == "disclaimer":
            disclaimer_lines.append(line)
        else:
            main_lines.append(line)

    signature_block = _collapse_lines(signature_lines)
    return {
        "main_body": _collapse_lines(main_lines),
        "forwarded_thread": _collapse_lines(forwarded_lines),
        "quoted_thread": _collapse_lines(quoted_lines),
        "signature_block": signature_block,
        "signature": signature_block,
        "disclaimer_block": _collapse_lines(disclaimer_lines),
    }


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

    return _collapse_lines(cleaned)


__all__ = ["clean_email_body", "segment_email_body"]
