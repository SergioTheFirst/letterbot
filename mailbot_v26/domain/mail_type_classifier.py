from __future__ import annotations

import re
from typing import Iterable, Protocol, Sequence


class AttachmentLike(Protocol):
    filename: str | None
    content_type: str


class MailTypeClassifier:
    """Deterministic mail type classifier based on message content."""

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
    URGENCY_KEYWORDS = {"срочно", "urgent", "asap", "важно", "immediately"}

    INVOICE_FINAL_KEYWORDS = {
        "финальн",
        "последн",
        "окончательн",
        "final notice",
        "final reminder",
        "final",
    }
    INVOICE_OVERDUE_KEYWORDS = {
        "просроч",
        "задолж",
        "overdue",
        "past due",
        "past-due",
        "late payment",
        "напоминание об оплате",
    }
    CONTRACT_AMENDMENT_KEYWORDS = {
        "доп соглаш",
        "доп. соглаш",
        "допсоглаш",
        "изменени",
        "amendment",
        "addendum",
    }
    CONTRACT_TERMINATION_KEYWORDS = {
        "расторжен",
        "termination",
        "terminate",
        "прекращени",
    }
    CONTRACT_NEW_KEYWORDS = {
        "новый договор",
        "заключение договора",
        "draft contract",
        "contract draft",
        "new contract",
    }
    REMINDER_ESCALATION_KEYWORDS = {
        "третье напоминание",
        "последнее напоминание",
        "ультиматум",
        "final notice",
        "final reminder",
        "third reminder",
    }
    REMINDER_FIRST_KEYWORDS = {
        "первое напоминание",
        "first reminder",
        "initial reminder",
    }
    CLAIM_COMPLAINT_KEYWORDS = {"претенз", "жалоб", "claim"}
    CLAIM_DISPUTE_KEYWORDS = {"dispute", "оспарив", "разногласи"}

    @classmethod
    def classify(
        cls,
        subject: str,
        body: str,
        attachments: Sequence[AttachmentLike] | None,
    ) -> str:
        mail_type, _ = cls._classify_base_detailed(subject, body, attachments)
        return mail_type

    @classmethod
    def classify_detailed(
        cls,
        subject: str,
        body: str,
        attachments: Sequence[AttachmentLike] | None,
        *,
        enable_hierarchy: bool = False,
    ) -> tuple[str, list[str]]:
        mail_type, reason_codes = cls._classify_base_detailed(subject, body, attachments)
        if enable_hierarchy:
            refined_type, refine_reasons = cls.refine_subtype(
                mail_type,
                subject=subject,
                body=body,
                attachments=attachments or [],
            )
            if refined_type != mail_type:
                reason_codes.extend(refine_reasons)
                return refined_type, reason_codes
        return mail_type, reason_codes

    @classmethod
    def refine_subtype(
        cls,
        mail_type: str,
        *,
        subject: str,
        body: str,
        attachments: Sequence[AttachmentLike],
    ) -> tuple[str, list[str]]:
        combined = f"{(subject or '').lower()} {(body or '').lower()}".strip()
        attachment_names = " ".join(
            (att.filename or "").lower() for att in attachments if att.filename
        )
        reasons: list[str] = []

        def match_keywords(markers: Iterable[str], text: str) -> list[str]:
            lowered = text.lower()
            return [marker for marker in sorted(markers, key=str) if marker in lowered]

        def add_match_reason(prefix: str, matches: list[str]) -> None:
            if matches:
                reasons.append(f"{prefix}={matches[0]}")

        if mail_type == "INVOICE":
            final_matches = match_keywords(cls.INVOICE_FINAL_KEYWORDS, combined)
            if final_matches:
                add_match_reason("mt.invoice.final.keyword", final_matches)
                return "INVOICE_FINAL", reasons
            overdue_matches = match_keywords(cls.INVOICE_OVERDUE_KEYWORDS, combined)
            if overdue_matches:
                add_match_reason("mt.invoice.overdue.keyword", overdue_matches)
                return "INVOICE_OVERDUE", reasons
            return mail_type, reasons

        if mail_type == "PAYMENT_REMINDER":
            escalation_matches = match_keywords(cls.REMINDER_ESCALATION_KEYWORDS, combined)
            if escalation_matches:
                add_match_reason("mt.reminder.escalation.keyword", escalation_matches)
                return "REMINDER_ESCALATION", reasons
            first_matches = match_keywords(cls.REMINDER_FIRST_KEYWORDS, combined)
            if first_matches:
                add_match_reason("mt.reminder.first.keyword", first_matches)
                return "REMINDER_FIRST", reasons
            if match_keywords(cls.URGENCY_KEYWORDS, combined):
                reasons.append("mt.reminder.escalation.urgency")
                return "REMINDER_ESCALATION", reasons
            return mail_type, reasons

        if mail_type in {"CONTRACT_APPROVAL", "CONTRACT_UPDATE"}:
            contract_text = f"{combined} {attachment_names}".strip()
            termination_matches = match_keywords(cls.CONTRACT_TERMINATION_KEYWORDS, contract_text)
            if termination_matches:
                add_match_reason("mt.contract.termination.keyword", termination_matches)
                return "CONTRACT_TERMINATION", reasons
            amendment_matches = match_keywords(cls.CONTRACT_AMENDMENT_KEYWORDS, contract_text)
            if amendment_matches:
                add_match_reason("mt.contract.amendment.keyword", amendment_matches)
                return "CONTRACT_AMENDMENT", reasons
            new_matches = match_keywords(cls.CONTRACT_NEW_KEYWORDS, contract_text)
            if new_matches:
                add_match_reason("mt.contract.new.keyword", new_matches)
                return "CONTRACT_NEW", reasons
            return mail_type, reasons

        if mail_type in {"UNKNOWN", "INFORMATION_ONLY"}:
            contract_matches = match_keywords(cls.CONTRACT_KEYWORDS, combined)
            if contract_matches:
                termination_matches = match_keywords(
                    cls.CONTRACT_TERMINATION_KEYWORDS, combined
                )
                if termination_matches:
                    add_match_reason("mt.contract.termination.keyword", termination_matches)
                    return "CONTRACT_TERMINATION", reasons
                amendment_matches = match_keywords(cls.CONTRACT_AMENDMENT_KEYWORDS, combined)
                if amendment_matches:
                    add_match_reason("mt.contract.amendment.keyword", amendment_matches)
                    return "CONTRACT_AMENDMENT", reasons
                new_matches = match_keywords(cls.CONTRACT_NEW_KEYWORDS, combined)
                if new_matches:
                    add_match_reason("mt.contract.new.keyword", new_matches)
                    return "CONTRACT_NEW", reasons

            dispute_matches = match_keywords(cls.CLAIM_DISPUTE_KEYWORDS, combined)
            if dispute_matches:
                add_match_reason("mt.claim.dispute.keyword", dispute_matches)
                return "CLAIM_DISPUTE", reasons
            complaint_matches = match_keywords(cls.CLAIM_COMPLAINT_KEYWORDS, combined)
            if complaint_matches:
                add_match_reason("mt.claim.complaint.keyword", complaint_matches)
                return "CLAIM_COMPLAINT", reasons

        return mail_type, reasons

    @classmethod
    def _classify_base_detailed(
        cls,
        subject: str,
        body: str,
        attachments: Sequence[AttachmentLike] | None,
    ) -> tuple[str, list[str]]:
        subject_lower = (subject or "").lower()
        body_lower = (body or "").lower()
        combined = f"{subject_lower} {body_lower}".strip()
        attachments = attachments or []
        reason_codes: list[str] = []

        has_amount = bool(
            re.search(r"\b\d{3,}(?:\s?руб|\s?rur|\s?usd|\s?eur)?\b", combined)
        )
        has_date = bool(re.search(r"\b\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?\b", combined))

        kinds = {cls._detect_attachment_kind(att.filename, att.content_type) for att in attachments}
        has_contract_att = "CONTRACT" in kinds
        has_invoice_att = "INVOICE" in kinds

        contract_match = cls._first_match(combined, cls.CONTRACT_KEYWORDS)
        approval_match = cls._first_match(combined, {"подпис", "утверд", "approve"})
        if contract_match and approval_match:
            reason_codes.append("mt.base=CONTRACT_APPROVAL")
            reason_codes.append(f"mt.contract.keyword={contract_match}")
            reason_codes.append(f"mt.contract.approval.keyword={approval_match}")
            return "CONTRACT_APPROVAL", reason_codes
        if has_contract_att:
            if approval_match:
                reason_codes.append("mt.base=CONTRACT_APPROVAL")
                reason_codes.append("mt.attachment_hint=contract_doc")
                reason_codes.append(f"mt.contract.approval.keyword={approval_match}")
                return "CONTRACT_APPROVAL", reason_codes
            reason_codes.append("mt.base=CONTRACT_UPDATE")
            reason_codes.append("mt.attachment_hint=contract_doc")
            return "CONTRACT_UPDATE", reason_codes

        invoice_match = cls._first_match(combined, cls.INVOICE_KEYWORDS)
        if invoice_match or has_invoice_att:
            reason_codes.append("mt.base=INVOICE")
            if invoice_match:
                reason_codes.append(f"mt.invoice.keyword={invoice_match}")
            if has_invoice_att:
                reason_codes.append("mt.attachment_hint=invoice_doc")
            return "INVOICE", reason_codes

        reminder_match = cls._first_match(combined, cls.PAYMENT_REMINDER_KEYWORDS)
        if reminder_match and (has_amount or has_date):
            reason_codes.append("mt.base=PAYMENT_REMINDER")
            reason_codes.append(f"mt.reminder.keyword={reminder_match}")
            if has_amount:
                reason_codes.append("mt.reminder.amount")
            if has_date:
                reason_codes.append("mt.reminder.date")
            return "PAYMENT_REMINDER", reason_codes

        price_match = cls._first_match(combined, cls.PRICE_KEYWORDS)
        if price_match:
            reason_codes.append("mt.base=PRICE_LIST")
            reason_codes.append(f"mt.price.keyword={price_match}")
            return "PRICE_LIST", reason_codes

        delivery_match = cls._first_match(combined, cls.DELIVERY_KEYWORDS)
        if delivery_match:
            reason_codes.append("mt.base=DELIVERY_NOTICE")
            reason_codes.append(f"mt.delivery.keyword={delivery_match}")
            return "DELIVERY_NOTICE", reason_codes

        security_match = cls._first_match(combined, cls.SECURITY_KEYWORDS)
        if security_match:
            reason_codes.append("mt.base=SECURITY_ALERT")
            reason_codes.append(f"mt.security.keyword={security_match}")
            return "SECURITY_ALERT", reason_codes

        policy_match = cls._first_match(combined, cls.POLICY_KEYWORDS)
        if policy_match:
            reason_codes.append("mt.base=POLICY_UPDATE")
            reason_codes.append(f"mt.policy.keyword={policy_match}")
            return "POLICY_UPDATE", reason_codes

        meeting_match = cls._first_match(combined, cls.MEETING_KEYWORDS)
        if meeting_match:
            reason_codes.append("mt.base=MEETING_CHANGE")
            reason_codes.append(f"mt.meeting.keyword={meeting_match}")
            return "MEETING_CHANGE", reason_codes

        deadline_match = cls._first_match(combined, cls.DEADLINE_KEYWORDS)
        if deadline_match and has_date:
            reason_codes.append("mt.base=DEADLINE_REMINDER")
            reason_codes.append(f"mt.deadline.keyword={deadline_match}")
            return "DEADLINE_REMINDER", reason_codes

        account_match = cls._first_match(combined, cls.ACCOUNT_KEYWORDS)
        if account_match:
            reason_codes.append("mt.base=ACCOUNT_CHANGE")
            reason_codes.append(f"mt.account.keyword={account_match}")
            return "ACCOUNT_CHANGE", reason_codes

        info_match = cls._first_match(combined, {"информ", "ознак", "for your information", "fyi"})
        if info_match:
            reason_codes.append("mt.base=INFORMATION_ONLY")
            reason_codes.append(f"mt.info.keyword={info_match}")
            return "INFORMATION_ONLY", reason_codes

        reason_codes.append("mt.base=UNKNOWN")
        return "UNKNOWN", reason_codes

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

    @staticmethod
    def _first_match(text: str, markers: Iterable[str]) -> str | None:
        lowered = text.lower()
        for marker in sorted(markers, key=str):
            if marker in lowered:
                return marker
        return None
