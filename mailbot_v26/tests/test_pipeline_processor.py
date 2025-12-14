import logging
from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def test_bank_invoice_marked_red():
    processor = _processor()
    msg = InboundMessage(
        subject="Счет на оплату услуг",
        sender="billing@bank.ru",
        body="Просим срочно оплатить счет до завтра, сумма 12000 руб.",
        attachments=[Attachment(filename="invoice.pdf", content=b"", content_type="application/pdf", text="Счет на оплату")],
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    result = processor.process("robot@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔴 СРОЧНО")


def test_contract_approval_marked_yellow():
    processor = _processor()
    msg = InboundMessage(
        subject="Согласование договора поставки",
        sender="manager@client.com",
        body="Просьба согласовать договор и вернуть подписанный экземпляр.",
        attachments=[Attachment(filename="contract.docx", content=b"", content_type="application/msword", text="Условия договора")],
        received_at=datetime(2024, 2, 2, 10, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🟡 ВАЖНО")


def test_hr_policy_info_blue():
    processor = _processor()
    msg = InboundMessage(
        subject="Обновление HR политики",
        sender="hr@company.com",
        body="Подготовили обновление корпоративной политики, ознакомьтесь на портале.",
        attachments=[],
        received_at=datetime(2024, 3, 3, 11, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔵 ИНФО")


def test_image_only_email_has_no_attachments():
    processor = _processor()
    msg = InboundMessage(
        subject="Фотографии",
        sender="studio@example.com",
        body="Смотрите снимки во вложении.",
        attachments=[Attachment(filename="photo.jpg", content=b"", content_type="image/jpeg", text="")],
        received_at=datetime(2024, 4, 4, 12, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    lines = result.split("\n")
    assert len(lines) == 2


def test_output_has_two_mandatory_lines():
    processor = _processor()
    msg = InboundMessage(
        subject="Напоминание",
        sender="team@example.com",
        body="Проверить статус задач и ответить клиенту.",
        attachments=[Attachment(filename="report.pdf", content=b"", content_type="application/pdf", text="Отчет по задачам")],
        received_at=datetime(2024, 5, 5, 13, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    mandatory = [line for line in result.split("\n") if line.strip()][0:2]
    assert len(mandatory) == 2
    assert mandatory[0].startswith(("🔴", "🟡", "🔵"))
    assert mandatory[1].split()[0] in MessageProcessor._VERB_ORDER


def test_no_duplicate_attachment_names():
    processor = _processor()
    msg = InboundMessage(
        subject="Отчеты и договор",
        sender="ops@example.com",
        body=(
            "Проверьте отчеты во вложении и обновленную версию договора, "
            "нужно подтвердить изменения."
        ),
        attachments=[
            Attachment(
                filename="report.pdf",
                content=b"",
                content_type="application/pdf",
                text="""
                Отчет по продажам за месяц включает показатели по регионам,
                динамику и ключевые выводы менеджмента для анализа.
                """,
            ),
            Attachment(
                filename="report.pdf",
                content=b"",
                content_type="application/pdf",
                text="""
                Дублирующий отчет с корректировками, содержит уточненные числа
                и обновленные итоговые данные по продажам.
                """,
            ),
            Attachment(
                filename="contract.docx",
                content=b"",
                content_type="application/msword",
                text="""
                Договор на поставку оборудования с описанием обязательств,
                сроков поставки и условий оплаты по контракту.
                """,
            ),
        ],
        received_at=datetime(2024, 7, 7, 15, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    lines = [line for line in result.split("\n") if line.strip()]
    attachment_lines = lines[2:]

    assert len(attachment_lines) == 2
    assert all(" — " in line for line in attachment_lines)

    filenames = [line.split(" — ")[0] for line in attachment_lines]
    assert len(filenames) == len(set(filenames))


def test_all_non_image_attachments_are_rendered():
    processor = _processor()
    msg = InboundMessage(
        subject="Пакет документов и таблиц",
        sender="ops@example.com",
        body="Высылаем комплект файлов",
        attachments=[
            Attachment(filename="contract.doc", content=b"", content_type="application/msword", text=""),
            Attachment(filename="note.docx", content=b"", content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", text="Короткая заметка"),
            Attachment(filename="prices.xlsx", content=b"", content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", text=""),
            Attachment(filename="report.xlsx", content=b"", content_type="application/vnd.ms-excel", text=""),
        ],
        received_at=datetime(2024, 8, 8, 16, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = result.split("\n")
    assert len(lines) >= 6
    assert lines[0].strip()
    assert lines[1].strip()
    assert lines[2] == ""

    attachment_lines = lines[3:7]
    assert len([ln for ln in attachment_lines if ln.strip()]) == 4

    filenames = [line.split(" — ")[0] for line in attachment_lines]
    assert len(filenames) == len(set(filenames))

    forbidden_phrases = [
        "нужно изучить",
        "можно просмотреть",
        "содержит информацию",
        "без подробностей",
    ]
    lowered = "\n".join(attachment_lines).lower()
    assert not any(phrase in lowered for phrase in forbidden_phrases)


def test_domain_priority_suggestion_does_not_change_priority(caplog):
    processor = _processor()
    msg = InboundMessage(
        subject="Hello friend",
        sender="friend@example.com",
        body="Hello friend, happy birthday dear friend!",
        attachments=[],
        received_at=datetime(2024, 6, 6, 14, 0),
    )

    with caplog.at_level(logging.INFO, logger="mailbot_v26.pipeline.processor"):
        result = processor.process("user@example.com", msg)

    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔵 ИНФО")
    assert any(
        "Domain priority suggestion: MEDIUM" in record.message for record in caplog.records
    )
