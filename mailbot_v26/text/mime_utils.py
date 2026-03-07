from __future__ import annotations

import logging
import re
import unicodedata
from email.header import decode_header
from typing import Iterable


logger = logging.getLogger(__name__)


def _decode_bytes(data: bytes, encodings: Iterable[str | None]) -> str:
    tried: list[str] = []
    for encoding in encodings:
        if not encoding or encoding in tried:
            continue
        tried.append(encoding)
        try:
            return data.decode(encoding, errors="strict")
        except (UnicodeDecodeError, LookupError):
            continue

    decoded = data.decode("utf-8", errors="ignore")
    logger.warning("Decoding used utf-8 with errors='ignore' after attempts: %s", ", ".join(tried))
    return decoded


def decode_bytes(data: bytes, charset_hint: str | None) -> str:
    """Decode raw bytes with charset fallbacks.

    The fallback order matches ``decode_mime_header`` expectations.
    """

    return _decode_bytes(
        data,
        (
            charset_hint,
            "utf-8",
            "cp1251",
            "koi8-r",
            "latin1",
        ),
    )


def _strip_control(text: str) -> str:
    normalized = unicodedata.normalize("NFC", text or "")
    return re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]", "", normalized)


def decode_mime_header(value: str) -> str:
    """Decode RFC 2047 headers reliably.

    - Uses ``decode_header`` to split parts
    - Fallback charset order: utf-8 → koi8-r → cp1251
    - Always returns a clean ``str`` without encoded-word artifacts
    """

    if not value:
        return ""

    parts: list[str] = []
    for chunk, encoding in decode_header(value):
        if isinstance(chunk, bytes):
            decoded = _decode_bytes(
                chunk, (encoding, "utf-8", "koi8-r", "cp1251")
            )
        else:
            decoded = chunk if chunk is not None else ""
        parts.append(decoded)

    joined = "".join(parts).strip()
    return _strip_control(joined)


__all__ = ["decode_mime_header", "decode_bytes"]
