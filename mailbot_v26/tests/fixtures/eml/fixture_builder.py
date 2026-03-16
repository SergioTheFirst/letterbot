from __future__ import annotations

from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AttachmentSpec:
    filename: str
    text: str
    content_type: str = "text/plain"


@dataclass(frozen=True, slots=True)
class EmlFixtureSpec:
    filename: str
    category: str
    sender: str
    subject: str
    body: str
    expected_render_mode: str = "full"
    expected_contains: tuple[str, ...] = ()
    expected_not_contains: tuple[str, ...] = ()
    attachments: tuple[AttachmentSpec, ...] = field(default_factory=tuple)
    date_header: str = "Tue, 02 Jan 2024 10:00:00 +0000"


FIXTURE_SPECS: tuple[EmlFixtureSpec, ...] = (
    EmlFixtureSpec(
        filename="invoice_simple.eml",
        category="invoice",
        sender="Billing Team <billing@billing.vendor.test>",
        subject="Invoice INV-DRY-01 for support services",
        body=(
            "Invoice INV-DRY-01.\n"
            "Amount due 12925 USD.\n"
            "Total payable 12925 USD.\n"
            "Payment due 2025-06-06.\n"
            "Please review the invoice details."
        ),
        expected_contains=("12 925", "billing", "INV-DRY-01"),
    ),
    EmlFixtureSpec(
        filename="invoice_with_attachment.eml",
        category="invoice",
        sender="AP Team <ap@billing.vendor.test>",
        subject="Invoice packet INV-ATT-01",
        body="Please review the attached invoice packet and payable total.",
        expected_contains=("7 800", "invoice", "INV-ATT-01"),
        attachments=(
            AttachmentSpec(
                filename="invoice_table.csv",
                content_type="text/csv",
                text=(
                    "Invoice INV-ATT-01\n"
                    "Item,Qty,Price,Amount\n"
                    "Support plan,1,5000 USD,5000 USD\n"
                    "Integration work,1,1500 USD,1500 USD\n"
                    "Subtotal,6500 USD\n"
                    "Tax,1300 USD\n"
                    "Total payable,7800 USD\n"
                    "Amount due,7800 USD\n"
                    "Payment due,2025-06-26\n"
                ),
            ),
        ),
    ),
    EmlFixtureSpec(
        filename="en_invoice_balance_due.eml",
        category="invoice",
        sender="Stripe-style Billing <billing@stripe-style.vendor.test>",
        subject="Invoice INV-4100 from Stripe-style billing",
        body=(
            "Invoice total 5,480 USD.\n"
            "Balance due 5,480 USD.\n"
            "Net 15.\n"
            "Pay by 28.03.2026."
        ),
        expected_contains=("5 480", "28.03.2026", "Pay"),
        expected_not_contains=("Review",),
    ),
    EmlFixtureSpec(
        filename="en_payment_reminder_overdue.eml",
        category="payment reminder",
        sender="Accounts Receivable <ar@collections.vendor.test>",
        subject="Second notice: invoice INV-9001 is overdue",
        body=(
            "Please pay the outstanding balance.\n"
            "Amount due 4,820 USD.\n"
            "Pay by 15.03.2026.\n"
            "Second notice."
        ),
        expected_contains=("4 820", "15.03.2026", "Pay"),
        expected_not_contains=("Review",),
    ),
    EmlFixtureSpec(
        filename="en_payment_reminder_final_notice.eml",
        category="payment reminder",
        sender="Billing Team <billing@xero-style.vendor.test>",
        subject="Final notice for invoice INV-9002",
        body=(
            "Outstanding balance 9,120 USD remains unpaid.\n"
            "Please pay by March 18, 2026.\n"
            "Final notice."
        ),
        expected_contains=("9 120", "18.03.2026", "Pay"),
        expected_not_contains=("Review",),
    ),
    EmlFixtureSpec(
        filename="payroll_standard.eml",
        category="payroll",
        sender="HR Robot <hr@hr.vendor.test>",
        subject="Расчетный листок за май",
        body="Во вложении расчетный листок за май. Пожалуйста, ознакомьтесь.",
        expected_contains=("Расчетный", "май", "HR"),
        expected_not_contains=("Оплатить",),
        attachments=(
            AttachmentSpec(
                filename="payroll.txt",
                text=(
                    "Расчетный листок за май\n"
                    "Начислено 93500 RUB\n"
                    "Удержано 15500 RUB\n"
                    "К выплате 78000 RUB\n"
                ),
            ),
        ),
    ),
    EmlFixtureSpec(
        filename="reconciliation.eml",
        category="reconciliation",
        sender="Reconciliation Desk <rec@reconciliation.vendor.test>",
        subject="Акт сверки взаимных расчетов REC-DRY-01",
        body="Во вложении акт сверки взаимных расчетов. Просьба проверить расхождения.",
        expected_contains=("REC-DRY-01", "сверки"),
        expected_not_contains=("Оплатить",),
        attachments=(
            AttachmentSpec(
                filename="reconciliation.txt",
                text=(
                    "Акт сверки взаимных расчетов REC-DRY-01\n"
                    "Начальный остаток 0 RUB\n"
                    "Обороты 19200 RUB\n"
                    "Конечный остаток 0 RUB\n"
                ),
            ),
        ),
    ),
    EmlFixtureSpec(
        filename="contract_amendment.eml",
        category="contract/amendment",
        sender="Legal Ops <legal@legal.vendor.test>",
        subject="Допсоглашение к договору CTR-DRY-01",
        body="Направляем договор и допсоглашение для проверки и согласования.",
        expected_contains=("договор", "CTR-DRY-01"),
        attachments=(
            AttachmentSpec(
                filename="contract_notes.txt",
                text=(
                    "Договор CTR-DRY-01\n"
                    "Соглашение о продлении срока действия.\n"
                    "Просим проверить правки и согласовать итоговую версию.\n"
                ),
            ),
        ),
    ),
    EmlFixtureSpec(
        filename="generic_notification.eml",
        category="generic notification",
        sender="Workspace Updates <notify@updates.vendor.test>",
        subject="Service update NOTICE-DRY-01",
        body=(
            "Status update for the shared workspace.\n"
            "Meeting moved to next week.\n"
            "Please note the new calendar details and updated meeting window."
        ),
        expected_contains=("NOTICE-DRY-01", "workspace"),
        expected_not_contains=("Оплатить",),
    ),
    EmlFixtureSpec(
        filename="reply_forward_polluted.eml",
        category="reply/forward polluted email",
        sender="Follow-up <followup@generic-mail.test>",
        subject="Re: Invoice POL-DRY-01 follow-up",
        body=(
            "Invoice POL-DRY-01.\n"
            "Amount due 21750 USD.\n"
            "Payment due 2025-08-05.\n"
            "Please review the invoice details.\n\n"
            "Forwarded message\n"
            "From: payroll@example.com\n"
            "Расчетный листок: начислено 999999 RUB.\n"
        ),
        expected_contains=("21 750", "POL-DRY-01"),
        expected_not_contains=("999999",),
    ),
    EmlFixtureSpec(
        filename="sender_ambiguous_content_clear.eml",
        category="sender ambiguous but content clear",
        sender="Contact <contact@generic-mail.test>",
        subject="Invoice GEN-DRY-01 for review",
        body=(
            "Invoice GEN-DRY-01.\n"
            "Amount due 23500 USD.\n"
            "Payment due 2025-08-25.\n"
            "Please review according to the payment terms."
        ),
        expected_contains=("23 500", "GEN-DRY-01"),
    ),
    EmlFixtureSpec(
        filename="sender_clear_content_weak.eml",
        category="sender clear but content weak",
        sender="Legal Team <weak@legal.vendor.test>",
        subject="Document packet WK-DRY-01",
        body="Please review the packet when convenient. Reference materials only.",
        expected_contains=("WK-DRY-01", "packet"),
        attachments=(
            AttachmentSpec(
                filename="packet_notes.txt",
                text="Reference packet with meeting notes and generic background materials.",
            ),
        ),
    ),
    EmlFixtureSpec(
        filename="table_heavy_attachment.eml",
        category="table-heavy attachment",
        sender="Billing Attachments <tables@billing.vendor.test>",
        subject="Invoice table INV-TBL-DRY-01",
        body="See attached invoice table with payable total.",
        expected_contains=("7 800", "INV-TBL-DRY-01"),
        attachments=(
            AttachmentSpec(
                filename="invoice_table_heavy.csv",
                content_type="text/csv",
                text=(
                    "Invoice INV-TBL-DRY-01\n"
                    "Item | Qty | Price | Amount\n"
                    "Support plan | 1 | 5000 USD | 5000 USD\n"
                    "Integration work | 1 | 1500 USD | 1500 USD\n"
                    "Subtotal 6500 USD\n"
                    "Tax 1300 USD\n"
                    "Total payable 7800 USD\n"
                    "Amount due 7800 USD\n"
                    "Payment due 2025-06-26\n"
                ),
            ),
        ),
    ),
)


def build_fixture_library(base_dir: Path | None = None) -> tuple[Path, ...]:
    target_dir = base_dir or Path(__file__).resolve().parent
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in FIXTURE_SPECS:
        message = EmailMessage()
        message["Subject"] = spec.subject
        message["From"] = spec.sender
        message["To"] = "receiver@example.com"
        message["Date"] = spec.date_header
        message["Message-ID"] = f"<{spec.filename}@letterbot.test>"
        message.set_content(spec.body)
        for attachment in spec.attachments:
            maintype, subtype = attachment.content_type.split("/", 1)
            message.add_attachment(
                attachment.text.encode("utf-8"),
                maintype=maintype,
                subtype=subtype,
                filename=attachment.filename,
            )
        fixture_path = target_dir / spec.filename
        fixture_path.write_bytes(message.as_bytes())
        written.append(fixture_path)
    return tuple(written)


__all__ = ["AttachmentSpec", "EmlFixtureSpec", "FIXTURE_SPECS", "build_fixture_library"]
