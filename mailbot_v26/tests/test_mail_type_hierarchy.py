from __future__ import annotations

import pytest

from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier


class DummyAttachment:
    def __init__(self, filename: str | None, content_type: str = "") -> None:
        self.filename = filename
        self.content_type = content_type


@pytest.mark.parametrize(
    ("subject", "body", "expected"),
    [
        ("Счет: финальное предупреждение", "Просьба оплатить", "INVOICE"),
        ("Третье напоминание о задолженности", "Сумма 1200 руб", "PAYMENT_REMINDER"),
        ("Претензия по договору", "Жалоба на качество", "UNKNOWN"),
    ],
)
def test_mail_type_hierarchy_flag_off(subject: str, body: str, expected: str) -> None:
    mail_type, reason_codes = MailTypeClassifier.classify_detailed(
        subject=subject,
        body=body,
        attachments=[],
        enable_hierarchy=False,
    )
    assert mail_type == expected
    assert reason_codes


@pytest.mark.parametrize(
    ("subject", "body", "attachments", "expected", "reason_token"),
    [
        (
            "Счет: финальное предупреждение",
            "Финальное уведомление об оплате",
            [DummyAttachment(filename="invoice.pdf")],
            "INVOICE_FINAL",
            "mt.invoice.final.keyword=финальн",
        ),
        (
            "Счет просрочен",
            "Просрочен платеж 1200 руб",
            [DummyAttachment(filename="invoice.pdf")],
            "INVOICE_OVERDUE",
            "mt.invoice.overdue.keyword=просроч",
        ),
        (
            "Доп. соглашение к договору",
            "",
            [],
            "CONTRACT_AMENDMENT",
            "mt.contract.amendment.keyword=доп. соглаш",
        ),
        (
            "Расторжение договора",
            "",
            [],
            "CONTRACT_TERMINATION",
            "mt.contract.termination.keyword=расторжен",
        ),
        (
            "Третье напоминание о задолженности",
            "Сумма 1200 руб, дата 12.12.2024",
            [],
            "REMINDER_ESCALATION",
            "mt.reminder.escalation.keyword=третье напоминание",
        ),
        (
            "Первое напоминание о задолженности",
            "Сумма 1200 руб",
            [],
            "REMINDER_FIRST",
            "mt.reminder.first.keyword=первое напоминание",
        ),
        (
            "Претензия по поставке",
            "Описание претензии по качеству",
            [],
            "CLAIM_COMPLAINT",
            "mt.claim.complaint.keyword=претенз",
        ),
        (
            "Dispute regarding delivery",
            "",
            [],
            "CLAIM_DISPUTE",
            "mt.claim.dispute.keyword=dispute",
        ),
    ],
)
def test_mail_type_hierarchy_flag_on(
    subject: str,
    body: str,
    attachments: list[DummyAttachment],
    expected: str,
    reason_token: str,
) -> None:
    mail_type, reason_codes = MailTypeClassifier.classify_detailed(
        subject=subject,
        body=body,
        attachments=attachments,
        enable_hierarchy=True,
    )
    assert mail_type == expected
    assert reason_codes
    assert reason_token in reason_codes
