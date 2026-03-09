"""Deterministic sender identity helpers for grouping and correction scope."""

from __future__ import annotations

import hashlib
import re
from typing import Any

_DISPLAY_NOISE_PREFIX_RE = re.compile(r"^(?:(?:re|fw|fwd)\s*:\s*)+", re.IGNORECASE)
_EMAIL_FRAGMENT_RE = re.compile(
    r"<[^>]+>|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    re.IGNORECASE,
)
_NON_WORD_RE = re.compile(r"[^\w]+", re.UNICODE)
_DOC_MARKER_RE = re.compile(r"[^0-9A-Za-z]+", re.UNICODE)
_GENERIC_LOCAL_PARTS = {
    "admin",
    "hello",
    "info",
    "mail",
    "noreply",
    "no reply",
    "notification",
    "notifications",
    "office",
    "support",
    "team",
}


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_words(value: Any) -> str:
    text = _DISPLAY_NOISE_PREFIX_RE.sub("", str(value or "").strip())
    text = _EMAIL_FRAGMENT_RE.sub(" ", text)
    text = _NON_WORD_RE.sub(" ", text)
    return " ".join(text.casefold().split())


def _normalize_doc_marker(value: Any) -> str:
    marker = _DOC_MARKER_RE.sub("", str(value or "").upper())
    return marker if len(marker) >= 6 else ""


def _normalize_subject_hint(value: Any) -> str:
    cleaned = _normalize_words(value)
    if not cleaned:
        return ""
    return " ".join(cleaned.split()[:3])


def _email_parts(sender_email: str) -> tuple[str, str]:
    normalized = _normalize_email(sender_email)
    if "@" not in normalized:
        return normalized, ""
    local, _, domain = normalized.partition("@")
    local = local.split("+", 1)[0].strip()
    return local, domain.strip().lower()


def normalize_sender_identity(
    sender_email: str,
    display_name: str = "",
    subject_hint: str = "",
    doc_marker: str = "",
) -> dict[str, str]:
    """
    Return a deterministic sender identity.

    The key is an addressing aid only. It never overrides content semantics.
    """

    normalized_email = _normalize_email(sender_email)
    local_part, domain = _email_parts(normalized_email)
    cleaned_display = _normalize_words(display_name)
    local_display = _normalize_words(local_part)
    marker = _normalize_doc_marker(doc_marker)
    cleaned_subject = _normalize_subject_hint(subject_hint)

    effective_display = cleaned_display
    confidence = "weak"

    if domain and cleaned_display:
        confidence = "strong"
    elif domain and marker:
        effective_display = marker.casefold()
        confidence = "medium"
    elif (
        domain
        and local_display
        and local_display not in _GENERIC_LOCAL_PARTS
        and not local_display.isdigit()
    ):
        effective_display = local_display
        confidence = "medium"
    elif domain and cleaned_subject and local_display in _GENERIC_LOCAL_PARTS:
        effective_display = cleaned_subject
        confidence = "medium"

    if domain and effective_display:
        key = f"{domain}||{effective_display}"
    else:
        key = normalized_email
        confidence = "weak"

    return {
        "key": key,
        "domain": domain,
        "display": effective_display,
        "confidence": confidence,
    }


def build_issuer_fingerprint(identity: dict[str, Any], template_class: str = "") -> str:
    """Return a short opaque fingerprint for stable sender grouping."""

    key = " ".join(str(identity.get("key") or "").strip().split()).casefold()
    template = " ".join(str(template_class or "").strip().split()).casefold()
    canonical = key if not template else f"{key}||{template}"
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]
    return f"issuer:{digest}"


def resolve_sender_profile_key(sender_email: str, **kwargs: Any) -> str:
    """Build a stable grouping fingerprint in one step."""

    identity = normalize_sender_identity(sender_email, **kwargs)
    return build_issuer_fingerprint(identity)


__all__ = [
    "build_issuer_fingerprint",
    "normalize_sender_identity",
    "resolve_sender_profile_key",
]

