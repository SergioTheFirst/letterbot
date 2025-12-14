from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.domain.domain_classifier import DomainClassifier, MailTypeClassifier
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def test_alpha_bank_invoice_marked_red_and_payment_request():
    processor = _processor()
    msg = InboundMessage(
        subject="Счет на оплату услуг",
        sender="billing@alpha-bank.ru",
        body="Просим оплатить до 12.12.2024 сумму 12000 руб.",
        attachments=[Attachment(filename="invoice.pdf", content=b"", content_type="application/pdf", text="Счет на оплату")],
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    domain = DomainClassifier.classify(msg.sender, "Alpha Bank", msg.subject)
    mail_type = MailTypeClassifier.classify(msg.subject, msg.body, msg.attachments, domain)

    assert domain == "BANK"
    assert mail_type == "PAYMENT_REQUEST"

    result = processor.process("robot@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔴 СРОЧНО")


def test_domain_registrar_expiry_yellow_extend():
    processor = _processor()
    msg = InboundMessage(
        subject="Домен example.ru истекает 01.02.2025",
        sender="notify@reg.ru",
        body="Оплатите продление домена до даты окончания.",
        attachments=[],
        received_at=datetime(2024, 2, 2, 10, 0),
    )

    domain = DomainClassifier.classify(msg.sender, "Reg.Ru", msg.subject)
    mail_type = MailTypeClassifier.classify(msg.subject, msg.body, msg.attachments, domain)

    assert domain == "DOMAIN_REGISTRAR"
    assert mail_type in {"DEADLINE_REMINDER", "PAYMENT_REMINDER", "INFORMATION_ONLY"}

    result = processor.process("user@example.com", msg)
    assert result is not None
    lines = result.split("\n")
    assert lines[0].startswith("🟡 ВАЖНО")
    assert lines[1].startswith("Продлить")


def test_hr_policy_update_blue_informational():
    processor = _processor()
    msg = InboundMessage(
        subject="Обновление HR политики",
        sender="hr@company.com",
        body="Подготовили обновление корпоративной политики, ознакомьтесь на портале.",
        attachments=[],
        received_at=datetime(2024, 3, 3, 11, 0),
    )

    domain = DomainClassifier.classify(msg.sender, "HR", msg.subject)
    mail_type = MailTypeClassifier.classify(msg.subject, msg.body, msg.attachments, domain)

    assert domain == "HR"
    assert mail_type == "POLICY_UPDATE"

    result = processor.process("user@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    second_line = result.split("\n")[1]
    assert first_line.startswith("🔵 ИНФО")
    assert second_line.startswith("Ознакомиться")


def test_client_contract_approval_yellow_sign():
    processor = _processor()
    msg = InboundMessage(
        subject="Согласование договора поставки",
        sender="manager@client.com",
        body="Просьба подписать договор и вернуть экземпляр.",
        attachments=[Attachment(filename="contract.docx", content=b"", content_type="application/msword", text="Условия договора")],
        received_at=datetime(2024, 4, 4, 12, 0),
    )

    domain = DomainClassifier.classify(msg.sender, "Client", msg.subject)
    mail_type = MailTypeClassifier.classify(msg.subject, msg.body, msg.attachments, domain)

    assert domain == "CLIENT"
    assert mail_type == "CONTRACT_APPROVAL"

    result = processor.process("user@example.com", msg)
    assert result is not None
    second_line = result.split("\n")[1]
    assert result.split("\n")[0].startswith("🟡 ВАЖНО")
    assert second_line.startswith("Подписать")


def test_family_email_not_red_without_urgency():
    processor = _processor()
    msg = InboundMessage(
        subject="Привет",
        sender="mom@example.com",
        body="Как дела? Позвони, когда будешь свободен.",
        attachments=[],
        received_at=datetime(2024, 5, 5, 13, 0),
    )

    domain = DomainClassifier.classify(msg.sender, "Mom", msg.subject)
    mail_type = MailTypeClassifier.classify(msg.subject, msg.body, msg.attachments, domain)

    assert domain == "FAMILY"
    assert mail_type == "INFORMATION_ONLY"

    result = processor.process("user@example.com", msg)
    assert result is not None
    assert not result.split("\n")[0].startswith("🔴")
