from __future__ import annotations

import hashlib
import re

_PREFIX_RE = re.compile(r"^(re|fwd|aw|ответ|пересл)\s*:\s*", re.IGNORECASE)


def normalize_subject(subject: str) -> str:
    if not subject:
        return ""
    working = subject.strip()
    while True:
        updated = _PREFIX_RE.sub("", working).strip()
        if updated == working:
            break
        working = updated
    working = re.sub(r"\s+", " ", working).strip()
    return working.lower()


def extract_message_ids(header: str | None) -> list[str]:
    if not header:
        return []
    value = str(header).strip()
    if not value:
        return []
    ids = [item.strip() for item in re.findall(r"<([^>]+)>", value) if item.strip()]
    if ids:
        return ids
    cleaned = re.sub(r"[<>]", " ", value)
    tokens = re.split(r"[\s,]+", cleaned)
    return [token.strip() for token in tokens if token.strip()]


def _first_message_id(header: str | None) -> str | None:
    ids = extract_message_ids(header)
    return ids[0] if ids else None


def compute_thread_key(
    account_email: str,
    rfc_message_id: str | None,
    in_reply_to: str | None,
    references: str | None,
    subject: str,
    from_email: str,
) -> str:
    try:
        account = (account_email or "").strip().lower()
        root = _first_message_id(references)
        if not root:
            root = _first_message_id(in_reply_to)
        if not root:
            root = _first_message_id(rfc_message_id) or (rfc_message_id or "").strip()
        if not root:
            fallback_subject = normalize_subject(subject or "")
            fallback_from = (from_email or "").strip().lower()
            root = f"{fallback_subject}|{fallback_from}"
        payload = f"{account}|{root}"
        return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()[:16]
    except Exception:
        return hashlib.sha256(b"").hexdigest()[:16]


__all__ = ["normalize_subject", "extract_message_ids", "compute_thread_key"]
