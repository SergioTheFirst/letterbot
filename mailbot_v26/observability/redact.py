"""Secret redaction for log output, delivery errors and support exports.

Telegram bot tokens travel inside request URLs (``/bot<TOKEN>/sendMessage``),
so ``str(exc)`` of a failed ``requests`` call carries the token. Everything
that can end up in logs, events, the database or Telegram text goes through
:func:`redact_secrets` first.
"""

from __future__ import annotations

import logging
import re

BOT_TOKEN_PLACEHOLDER = "bot<redacted>"
BARE_TOKEN_PLACEHOLDER = "<redacted-token>"

# ``bot<digits>:<secret>`` as it appears in Bot API URL paths.
_BOT_URL_TOKEN_RE = re.compile(r"bot\d+:[A-Za-z0-9_-]+")
# A bare ``<bot id>:<secret>`` token outside a URL.
_BARE_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9_])\d{6,}:[A-Za-z0-9_-]{30,}(?![A-Za-z0-9_-])"
)


def redact_secrets(text: object) -> str:
    """Return ``text`` as a string with Telegram bot tokens masked."""
    if text is None:
        return ""
    value = str(text)
    if not value:
        return value
    value = _BOT_URL_TOKEN_RE.sub(BOT_TOKEN_PLACEHOLDER, value)
    return _BARE_TOKEN_RE.sub(BARE_TOKEN_PLACEHOLDER, value)


class RedactingFormatter(logging.Formatter):
    """Formatter that masks secrets in the whole record, tracebacks included."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_secrets(super().format(record))


__all__ = [
    "BARE_TOKEN_PLACEHOLDER",
    "BOT_TOKEN_PLACEHOLDER",
    "RedactingFormatter",
    "redact_secrets",
]
