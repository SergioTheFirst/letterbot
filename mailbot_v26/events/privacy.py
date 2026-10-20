"""Privacy helpers for event payloads (pure functions, stdlib only).

CONSTITUTION VII: events never store a raw email subject, body or summary.
Payloads keep derived facts instead:

- ``subject_words`` / ``summary_words``: word counts (weekly attention),
- ``text_markers``: which words of a fixed list occurred (sender trust score),
- ``subject_hash``: keyed hash of the normalized subject (reminder chains in
  the legacy ``events`` table; the key stays on this computer, see
  :mod:`mailbot_v26.storage.privacy_key`).

Every scrub function returns a new dict, never mutates its input, is
idempotent and masks Telegram bot tokens in all strings, so live emitters,
the events backfill and the one-time scrub migration can share it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from typing import Any

from mailbot_v26.events.contract import EventType
from mailbot_v26.observability.redact import redact_secrets

# Words the sender trust score looks for in event payload text
# (KnowledgeAnalytics._build_sender_relationship_profile). Kept sorted.
TEXT_MARKERS: tuple[str, ...] = ("dispute", "fast reply", "invoice", "paid")

# Raw email text dropped from every payload (at any nesting depth).
RAW_TEXT_KEYS: frozenset[str] = frozenset({"subject", "body_summary"})
# A raw summary is replaced by its length under SUMMARY_CHARS_KEY.
SUMMARY_KEY = "summary"
SUMMARY_CHARS_KEY = "summary_chars"

SUBJECT_HASH_HEX_CHARS = 32
LEGACY_SUBJECT_HASH_TYPE = "email_received"

_REPLY_PREFIX_RE = re.compile(r"^(re:|fw:|fwd:)\s*", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_WORD_RE = re.compile(r"\b\w+\b")


def normalize_reminder_subject(subject: str) -> str:
    """Subject as reminder-chain detection compares it.

    Strips one leading ``Re:``/``Fw:``/``Fwd:``, collapses whitespace and
    lowercases.
    """
    cleaned = _REPLY_PREFIX_RE.sub("", subject.strip())
    cleaned = _WHITESPACE_RE.sub(" ", cleaned)
    return cleaned.lower()


def subject_hash(subject: object, key: bytes) -> str:
    """HMAC-SHA256 of the normalized subject, 32 hex chars; "" when blank.

    For one key, equal hashes mean equal normalized subjects; without the key
    a guessed subject cannot be confirmed from a stored hash.
    """
    if not key:
        raise ValueError("subject hash key is empty")
    normalized = normalize_reminder_subject(str(subject or ""))
    if not normalized:
        return ""
    digest = hmac.new(
        bytes(key), normalized.encode("utf-8", "surrogatepass"), hashlib.sha256
    )
    return digest.hexdigest()[:SUBJECT_HASH_HEX_CHARS]


def count_words(text: object) -> int:
    return len(_WORD_RE.findall(str(text or "")))


def subject_words(subject: object) -> int | None:
    """Words in a subject; None when it is blank (weekly attention skips it)."""
    return _word_count_unless_blank(subject)


def summary_words(summary: object) -> int | None:
    """Words in a body summary; None when it is blank."""
    return _word_count_unless_blank(summary)


def _word_count_unless_blank(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return count_words(text)


def text_markers(*values: object) -> list[str]:
    """TEXT_MARKERS found in ``values``, in TEXT_MARKERS order.

    The trust score runs substring tests over the lowercased JSON payload
    text, so each value is searched in exactly that form
    (``json.dumps(value, ensure_ascii=False)``): the markers of a raw field
    give the same test results as the field itself.
    """
    haystacks = [
        json.dumps(value, ensure_ascii=False, default=str).lower()
        for value in values
        if value is not None
    ]
    return [
        marker
        for marker in TEXT_MARKERS
        if any(marker in haystack for haystack in haystacks)
    ]


def redact_payload(value: Any) -> Any:
    """``value`` with Telegram bot tokens masked in every string (recursive)."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, Mapping):
        return {key: redact_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_payload(item) for item in value]
    return value


def _drop_raw_text(value: Any) -> Any:
    if isinstance(value, Mapping):
        cleaned: dict[Any, Any] = {}
        for key, item in value.items():
            if key in RAW_TEXT_KEYS:
                continue
            if key == SUMMARY_KEY:
                cleaned[SUMMARY_CHARS_KEY] = len(str(item or ""))
                continue
            cleaned[key] = _drop_raw_text(item)
        return cleaned
    if isinstance(value, (list, tuple)):
        return [_drop_raw_text(item) for item in value]
    return value


def _scrub_common(payload: Mapping[str, Any]) -> dict[str, Any]:
    return redact_payload(_drop_raw_text(payload))


def _type_value(event_type: EventType | str) -> str:
    return event_type.value if isinstance(event_type, EventType) else str(event_type)


def scrub_email_received(payload: Mapping[str, Any]) -> dict[str, Any]:
    """EMAIL_RECEIVED payload (events_v1) with derived facts instead of raw text.

    ``subject``/``body_summary`` become ``subject_words``/``summary_words``
    (present only for non-blank text, the way weekly attention reads them)
    and ``text_markers``. A payload without raw text keeps its derived fields.
    """
    scrubbed = _scrub_common(payload)
    if not any(key in payload for key in RAW_TEXT_KEYS):
        return scrubbed
    subject = payload.get("subject")
    summary = payload.get("body_summary")
    for key, count in (
        ("subject_words", subject_words(subject)),
        ("summary_words", summary_words(summary)),
    ):
        if count is None:
            scrubbed.pop(key, None)
        else:
            scrubbed[key] = count
    scrubbed["text_markers"] = text_markers(subject, summary)
    return scrubbed


def scrub_payload(
    event_type: EventType | str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    """Privacy scrub of one events_v1 payload."""
    if _type_value(event_type) == EventType.EMAIL_RECEIVED.value:
        return scrub_email_received(payload)
    return _scrub_common(payload)


def scrub_legacy_payload(
    event_type: str, payload: Mapping[str, Any], *, subject_key: bytes | None
) -> dict[str, Any]:
    """Privacy scrub of one legacy ``events`` payload.

    ``email_received`` keeps reminder-chain matching through ``subject_hash``;
    without a key no hash is stored (cross-sender subject matching is then
    off for that row). Every other subject is dropped.
    """
    scrubbed = _scrub_common(payload)
    if (
        str(event_type) == LEGACY_SUBJECT_HASH_TYPE
        and "subject" in payload
        and subject_key
    ):
        scrubbed["subject_hash"] = subject_hash(payload.get("subject"), subject_key)
    return scrubbed


__all__ = [
    "LEGACY_SUBJECT_HASH_TYPE",
    "RAW_TEXT_KEYS",
    "SUBJECT_HASH_HEX_CHARS",
    "SUMMARY_CHARS_KEY",
    "SUMMARY_KEY",
    "TEXT_MARKERS",
    "count_words",
    "normalize_reminder_subject",
    "redact_payload",
    "scrub_email_received",
    "scrub_legacy_payload",
    "scrub_payload",
    "subject_hash",
    "subject_words",
    "summary_words",
    "text_markers",
]
