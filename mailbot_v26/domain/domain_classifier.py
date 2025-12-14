from __future__ import annotations

import re
from typing import Iterable, Protocol, Sequence


class AttachmentLike(Protocol):
    filename: str | None
    content_type: str


class DomainClassifier:
    """Keyword-driven domain classifier with deterministic scoring."""

    _THRESHOLD = 2
    _CATEGORY_KEYWORDS = {
        "BANK": {
            "bank",
            "payment",
            "transfer",
            "account",
            "card",
            "loan",
        },
        "TAX": {
            "tax",
            "vat",
            "fns",
            "налог",
            "ндс",
        },
        "LEGAL": {
            "legal",
            "court",
            "lawsuit",
            "attorney",
            "subpoena",
            "иск",
            "арбитраж",
        },
        "CONTRACT": {
            "contract",
            "agreement",
            "подпис",
            "договор",
            "соглашение",
        },
        "INVOICE": {
            "invoice",
            "bill",
            "billing",
            "счет",
            "счёт",
            "оплат",
        },
        "PRICE_LIST": {
            "price list",
            "pricelist",
            "прайс",
            "цен",
        },
        "HR": {
            "hr",
            "hiring",
            "candidate",
            "vacation",
            "отпуск",
            "кадры",
            "salary",
            "payroll",
        },
        "LOGISTICS": {
            "delivery",
            "shipment",
            "logistics",
            "transport",
            "cargo",
            "tracking",
            "достав",
            "груз",
        },
        "PERSONAL": {
            "hello",
            "привет",
            "family",
            "friend",
            "birthday",
            "поздрав",
        },
        "MARKETING": {
            "sale",
            "discount",
            "offer",
            "promotion",
            "promo",
            "limited",
        },
    }

    @classmethod
    def classify(cls, sender: str, subject: str, body: str) -> str:
        text = " ".join(filter(None, [sender, subject, body])).lower()

        best_category = "UNKNOWN"
        best_score = 0
        for category, keywords in cls._CATEGORY_KEYWORDS.items():
            score = cls._score(text, keywords)
            if score > best_score:
                best_category = category
                best_score = score

        if best_score < cls._THRESHOLD:
            return "UNKNOWN"
        return best_category

    @staticmethod
    def _score(text: str, keywords: Iterable[str]) -> int:
        score = 0
        for keyword in keywords:
            score += text.count(keyword)
        return score


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
