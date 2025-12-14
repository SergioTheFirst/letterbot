from __future__ import annotations

import re
from typing import Iterable, Protocol, Sequence


class AttachmentLike(Protocol):
    filename: str | None
    content_type: str


class DomainClassifier:
    """Deterministic classification for sender domain."""

    BANK_KEYWORDS = {"bank", "sber", "alpha", "alfa", "tinkoff", "vtb"}
    TAX_KEYWORDS = {"nalog", "fns", "tax"}
    COURT_KEYWORDS = {"sud", "court", "arbitr"}
    GOVERNMENT_KEYWORDS = {"gov", "gos", "min"}
    DOMAIN_REGISTRAR_KEYWORDS = {"reg.ru", "nic.ru", "godaddy", "namecheap"}
    HR_KEYWORDS = {"hr", "otdelkadrov", "kadry"}
    IT_KEYWORDS = {"it", "support", "helpdesk", "service"}
    LOGISTICS_KEYWORDS = {"delivery", "logist", "transport"}
    FAMILY_SENDERS = {"mom@example.com", "dad@example.com", "spouse@example.com"}

    @classmethod
    def classify(cls, sender_email: str, sender_name: str, subject: str) -> str:
        email_lower = (sender_email or "").lower()
        subject_lower = (subject or "").lower()
        domain = email_lower.split("@")[-1]

        if email_lower in cls.FAMILY_SENDERS or "family" in domain:
            return "FAMILY"

        if cls._contains_any(domain, cls.BANK_KEYWORDS):
            return "BANK"
        if cls._contains_any(domain, cls.TAX_KEYWORDS):
            return "TAX"
        if cls._contains_any(domain, cls.COURT_KEYWORDS):
            return "COURT"
        if cls._contains_any(domain, cls.GOVERNMENT_KEYWORDS):
            return "GOVERNMENT"
        if cls._contains_any(domain, {"client"}):
            return "CLIENT"
        if cls._contains_any(domain, {"supplier", "vendor", "postav"}):
            return "SUPPLIER"
        if cls._contains_any(domain, cls.HR_KEYWORDS) or cls._contains_any(subject_lower, {"отпуск", "vacation", "кадры", "hr"}):
            return "HR"
        if cls._contains_any(domain, cls.IT_KEYWORDS):
            return "IT"
        if cls._contains_any(domain, cls.DOMAIN_REGISTRAR_KEYWORDS):
            return "DOMAIN_REGISTRAR"
        if cls._contains_any(domain, cls.LOGISTICS_KEYWORDS):
            return "LOGISTICS"

        local_part = email_lower.split("@")[0]
        if local_part.startswith(("team", "info", "noreply")):
            return "INTERNAL"

        return "UNKNOWN"

    @staticmethod
    def _contains_any(text: str, markers: Iterable[str]) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in markers)


class MailTypeClassifier:
    """Deterministic mail type classifier."""

    INVOICE_KEYWORDS = {"счет", "счёт", "invoice", "bill", "оплат"}
    PAYMENT_REMINDER_KEYWORDS = {"напомин", "просроч", "долг", "ожидаем"}
    CONTRACT_KEYWORDS = {"договор", "contract", "соглашение", "agreement"}
    PRICE_KEYWORDS = {"прайс", "price list", "стоимост", "цен"}
    DELIVERY_KEYWORDS = {"достав", "отгруз", "shipment", "груз"}
    DEADLINE_KEYWORDS = {"срок", "deadline", "истекает", "дата"}
    SECURITY_KEYWORDS = {"подозр", "взлом", "security", "парол"}
    POLICY_KEYWORDS = {"policy", "политик", "обновление", "update"}
    MEETING_KEYWORDS = {"встреч", "meeting", "совещани", "перенос"}
    ACCOUNT_KEYWORDS = {"аккаунт", "учетн", "учётн", "account"}

    @classmethod
    def classify(
        cls,
        subject: str,
        body: str,
        attachments: Sequence[AttachmentLike] | None,
        domain: str,
    ) -> str:
        subject_lower = (subject or "").lower()
        body_lower = (body or "").lower()
        combined = f"{subject_lower} {body_lower}".strip()
        attachments = attachments or []

        has_amount = bool(re.search(r"\b\d{3,}(?:\s?руб|\s?rur|\s?usd|\s?eur)?\b", combined))
        has_date = bool(re.search(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", combined))

        kinds = {cls._detect_attachment_kind(att.filename, att.content_type) for att in attachments}
        has_contract_att = "CONTRACT" in kinds
        has_invoice_att = "INVOICE" in kinds

        if domain == "DOMAIN_REGISTRAR":
            if cls._contains_any(combined, {"истекает", "expire", "expirat"}):
                return "DEADLINE_REMINDER"
            return "INFORMATION_ONLY"

        if domain == "FAMILY":
            return "INFORMATION_ONLY"

        if domain == "BANK" and (has_invoice_att or cls._contains_any(combined, cls.INVOICE_KEYWORDS) or (has_amount and has_date)):
            return "PAYMENT_REQUEST"

        if cls._contains_any(combined, cls.CONTRACT_KEYWORDS) and cls._contains_any(combined, {"подпис", "утверд", "approve"}):
            return "CONTRACT_APPROVAL"
        if has_contract_att:
            return "CONTRACT_APPROVAL" if cls._contains_any(combined, {"подпис", "approve"}) else "CONTRACT_UPDATE"

        if cls._contains_any(combined, cls.INVOICE_KEYWORDS) or has_invoice_att:
            return "INVOICE"

        if cls._contains_any(combined, cls.PAYMENT_REMINDER_KEYWORDS) and (has_amount or has_date):
            return "PAYMENT_REMINDER"

        if cls._contains_any(combined, cls.PRICE_KEYWORDS):
            return "PRICE_LIST"

        if cls._contains_any(combined, cls.DELIVERY_KEYWORDS):
            return "DELIVERY_NOTICE"

        if cls._contains_any(combined, cls.SECURITY_KEYWORDS):
            return "SECURITY_ALERT"

        if cls._contains_any(combined, cls.POLICY_KEYWORDS) and domain == "HR":
            return "POLICY_UPDATE"

        if cls._contains_any(combined, cls.MEETING_KEYWORDS):
            return "MEETING_CHANGE"

        if cls._contains_any(combined, cls.DEADLINE_KEYWORDS) and has_date:
            return "DEADLINE_REMINDER"

        if cls._contains_any(combined, cls.ACCOUNT_KEYWORDS):
            return "ACCOUNT_CHANGE"

        if cls._contains_any(combined, {"информ", "ознак", "for your information", "fyi"}):
            return "INFORMATION_ONLY"

        return "UNKNOWN"

    @staticmethod
    def _detect_attachment_kind(filename: str | None, content_type: str = "") -> str:
        lower_ct = (content_type or "").lower()
        lower = (filename or "").lower()
        if lower.endswith((".xls", ".xlsx")) or "excel" in lower_ct:
            return "EXCEL"
        if lower.endswith((".doc", ".docx")) or "word" in lower_ct:
            return "CONTRACT"
        if lower.endswith(".pdf") or "pdf" in lower_ct:
            return "PDF"
        if any(token in lower for token in ("invoice", "bill", "счет", "счёт")):
            return "INVOICE"
        return "GENERIC"

    @staticmethod
    def _contains_any(text: str, markers: Iterable[str]) -> bool:
        lowered = text.lower()
        return any(marker in lowered for marker in markers)
