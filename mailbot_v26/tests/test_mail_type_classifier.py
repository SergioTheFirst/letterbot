from mailbot_v26.domain.mail_type_classifier import MailTypeClassifier


def test_classifies_english_payment_reminder_before_generic_invoice() -> None:
    mail_type = MailTypeClassifier.classify(
        subject="Second notice: invoice INV-9001 is overdue",
        body=(
            "Please pay the outstanding balance. "
            "Amount due 4,820 USD. Pay by 15.03.2026. Second notice."
        ),
        attachments=[],
    )

    assert mail_type == "PAYMENT_REMINDER"


def test_classifies_docusign_signature_request_as_contract_approval() -> None:
    mail_type = MailTypeClassifier.classify(
        subject="DocuSign: Please sign MSA-44",
        body=(
            "Please review and sign the agreement by 21.03.2026. "
            "Attached is the final contract version."
        ),
        attachments=[],
    )

    assert mail_type == "CONTRACT_APPROVAL"


def test_meeting_change_beats_generic_policy_update_marker() -> None:
    mail_type = MailTypeClassifier.classify(
        subject="Meeting rescheduled to Thursday",
        body=(
            "The budget review meeting was moved to Thursday 21.03.2026 at 15:00. "
            "Please use the updated invite."
        ),
        attachments=[],
    )

    assert mail_type == "MEETING_CHANGE"


def test_reference_invoice_without_payment_does_not_classify_as_invoice() -> None:
    mail_type = MailTypeClassifier.classify(
        subject="Update on invoice thread and contract appendix",
        body=(
            "For your information, we attached the revised appendix and the historic "
            "invoice copy. No payment is needed today; please just keep the thread "
            "for reference."
        ),
        attachments=[],
    )

    assert mail_type != "INVOICE"


def test_future_invoice_notice_without_amount_does_not_classify_as_invoice() -> None:
    mail_type = MailTypeClassifier.classify(
        subject="Нужно согласование закупки до 20.03.2026",
        body=(
            "Просим согласовать закупку лицензий до 20.03.2026. "
            "После подтверждения отправим счет поставщику."
        ),
        attachments=[],
    )

    assert mail_type != "INVOICE"
