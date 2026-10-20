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

# The ":" between bot id and secret, raw or percent-encoded once (%3A: a URL
# quoted into a query string or an error page) or twice (%253A).
_TOKEN_SEPARATOR = r"(?::|%3[Aa]|%253[Aa])"
# ``bot<digits>:<secret>`` as it appears in Bot API URL paths.
_BOT_URL_TOKEN_RE = re.compile(r"bot\d+" + _TOKEN_SEPARATOR + r"[A-Za-z0-9_-]+")
# A bare ``<bot id>:<secret>`` token outside a URL. Its only left bound is
# that the id does not continue a longer number: a letter may come right
# before it (the hex digit of a %2F escape, a repr() escape such as \n, a
# "Bot" prefix).
_BARE_TOKEN_RE = re.compile(
    r"(?<!\d)\d{6,}" + _TOKEN_SEPARATOR + r"[A-Za-z0-9_-]{30,}(?![A-Za-z0-9_-])"
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
