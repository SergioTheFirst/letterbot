"""Deterministic issuer-profile helpers built on canonical interpretation inputs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

_GENERIC_SENDER_DOMAINS = {
    "gmail.com",
    "hotmail.com",
    "icloud.com",
    "mail.ru",
    "outlook.com",
    "yahoo.com",
    "yandex.ru",
    "generic-mail.test",
}
_ISSUER_ID_RE = re.compile(
    r"\b(?:инн|vat|tax id)[^\dA-Za-z]{0,4}([A-Z0-9]{8,14})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class IssuerProfile:
    issuer_key: str
    issuer_label: str
    sender_email: str | None = None
    sender_domain: str | None = None
    issuer_tax_id: str | None = None


def _normalize_token(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def extract_sender_domain(sender_email: str | None) -> str | None:
    normalized_sender = _normalize_email(sender_email)
    if "@" not in normalized_sender:
        return None
    domain = normalized_sender.split("@", 1)[1].strip().lower()
    return domain or None


def _extract_issuer_tax_id(
    *,
    subject: str,
    body_text: str,
    attachment_names: Iterable[str],
    attachment_text: str,
    message_facts: Mapping[str, Any] | None = None,
) -> str | None:
    combined = " ".join(
        part
        for part in (
            subject,
            body_text,
            " ".join(str(item or "") for item in attachment_names),
            attachment_text,
            " ".join(
                str(value)
                for value in (message_facts or {}).values()
                if isinstance(value, (str, int, float))
            ),
        )
        if part
    )
    match = _ISSUER_ID_RE.search(combined)
    if not match:
        return None
    return match.group(1).strip().upper() or None


def build_issuer_profile(
    *,
    sender_email: str,
    subject: str = "",
    body_text: str = "",
    attachment_names: Iterable[str] = (),
    attachment_text: str = "",
    message_facts: Mapping[str, Any] | None = None,
) -> IssuerProfile | None:
    normalized_sender = _normalize_email(sender_email)
    sender_domain = extract_sender_domain(normalized_sender)
    issuer_tax_id = _extract_issuer_tax_id(
        subject=subject,
        body_text=body_text,
        attachment_names=attachment_names,
        attachment_text=attachment_text,
        message_facts=message_facts,
    )
    if issuer_tax_id:
        return IssuerProfile(
            issuer_key=f"tax:{issuer_tax_id}",
            issuer_label=issuer_tax_id,
            sender_email=normalized_sender or None,
            sender_domain=sender_domain,
            issuer_tax_id=issuer_tax_id,
        )
    if sender_domain and sender_domain not in _GENERIC_SENDER_DOMAINS:
        return IssuerProfile(
            issuer_key=f"domain:{sender_domain}",
            issuer_label=sender_domain,
            sender_email=normalized_sender or None,
            sender_domain=sender_domain,
            issuer_tax_id=None,
        )
    if normalized_sender:
        return IssuerProfile(
            issuer_key=f"email:{normalized_sender}",
            issuer_label=normalized_sender,
            sender_email=normalized_sender,
            sender_domain=sender_domain,
            issuer_tax_id=None,
        )
    return None


def issuer_profile_from_interpretation_payload(
    payload: Mapping[str, Any],
) -> IssuerProfile | None:
    issuer_key = _normalize_token(payload.get("issuer_key"))
    issuer_label = " ".join(str(payload.get("issuer_label") or "").split()).strip()
    sender_email = _normalize_email(payload.get("sender_email"))
    sender_domain = _normalize_token(
        payload.get("issuer_domain") or extract_sender_domain(sender_email)
    )
    issuer_tax_id = (
        " ".join(str(payload.get("issuer_tax_id") or "").split()).strip().upper() or None
    )
    if issuer_key and issuer_label:
        return IssuerProfile(
            issuer_key=issuer_key,
            issuer_label=issuer_label,
            sender_email=sender_email or None,
            sender_domain=sender_domain or None,
            issuer_tax_id=issuer_tax_id,
        )
    return build_issuer_profile(
        sender_email=sender_email,
        message_facts=payload,
    )


__all__ = [
    "IssuerProfile",
    "build_issuer_profile",
    "extract_sender_domain",
    "issuer_profile_from_interpretation_payload",
]
