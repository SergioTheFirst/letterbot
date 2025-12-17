from datetime import datetime
import re
from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _strip_html(line: str) -> str:
    return re.sub(r"</?[^>]+>", "", line)


def _attachment_lines(result: str, names: set[str]) -> list[str]:
    lines = [_strip_html(line) for line in result.split("\n") if line.strip()]
    return [line for line in lines if any(line.startswith(name) for name in names)]


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
    assert first_line.startswith("🔴 от")


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
    assert first_line.startswith("🟡 от")


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
    assert first_line.startswith("🔵 от")


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
    assert len(lines) >= 2


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
    mandatory = [line for line in result.split("\n") if line.strip()][0:3]
    assert len(mandatory) >= 2
    assert mandatory[0].startswith(("🔴", "🟡", "🔵"))
    assert mandatory[1].startswith("<b>")
    assert mandatory[2].split()[0] in MessageProcessor._VERB_ORDER


def test_no_duplicate_attachment_names():
    processor = _processor()
    attachments = [
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
    ]

    msg = InboundMessage(
        subject="Отчеты и договор",
        sender="ops@example.com",
        body=(
            "Проверьте отчеты во вложении и обновленную версию договора, "
            "нужно подтвердить изменения."
        ),
        attachments=attachments,
        received_at=datetime(2024, 7, 7, 15, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    assert len(attachment_lines) == 3
    filenames = [line.split(" — ")[0] for line in attachment_lines]
    assert filenames.count("report.pdf") == 2
    assert filenames.count("contract.docx") == 1


def test_all_non_image_attachments_are_rendered():
    processor = _processor()
    attachments = [
        Attachment(
            filename="contract.doc",
            content=b"",
            content_type="application/msword",
            text="Общие условия договора на поставку продукции.",
        ),
        Attachment(
            filename="note.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Короткая заметка",
        ),
        Attachment(
            filename="prices.xlsx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Таблица с ценами и кодами товаров",
        ),
        Attachment(
            filename="report.xlsx",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="Отчет по продажам за квартал",
        ),
    ]

    msg = InboundMessage(
        subject="Пакет документов и таблиц",
        sender="ops@example.com",
        body="Высылаем комплект файлов",
        attachments=attachments,
        received_at=datetime(2024, 8, 8, 16, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = result.split("\n")
    assert lines[0].strip()
    assert lines[1].strip()

    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)
    assert len(attachment_lines) == 4

    for filename in ["contract.doc", "note.docx", "prices.xlsx", "report.xlsx"]:
        assert any(line.startswith(filename) for line in attachment_lines)


def test_informational_email_remains_blue():
    processor = _processor()
    msg = InboundMessage(
        subject="Hello friend",
        sender="friend@example.com",
        body="Hello friend, happy birthday dear friend!",
        attachments=[],
        received_at=datetime(2024, 6, 6, 14, 0),
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔵 от")


def test_attachment_lines_drop_prefixes_and_counts():
    processor = _processor()
    attachments = [
        Attachment(
            filename="stats.xlsx",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="Код;Цена;Сумма\n1;10;10\n2;20;40",
        ),
        Attachment(
            filename="notes.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Проверка с документами",
        ),
    ]

    msg = InboundMessage(
        subject="Таблицы и документы",
        sender="ops@example.com",
        body="",
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    assert len(attachment_lines) == 2
    for line in attachment_lines:
        assert "таблица:" not in line
        assert "(≈" not in line


def test_body_placeholder_is_silent_when_empty():
    processor = _processor()
    msg = InboundMessage(
        subject="Файлы",
        sender="sender@example.com",
        body="",
        attachments=[Attachment(filename="info.pdf", content=b"", content_type="application/pdf", text="Вложение")],
    )

    result = processor.process("user@example.com", msg)

    lines = result.split("\n")
    assert len(lines) >= 3
    assert "тело" not in result.lower()
    assert any(line.startswith("info.pdf") for line in _attachment_lines(result, {"info.pdf"}))


def test_normalize_action_subject_deduplicates_tokens():
    processor = _processor()
    action_one = processor._normalize_action_subject(
        "Проверить", "Прайс лист", [], ""
    )
    assert action_one == "Проверить цены"

    action_two = processor._normalize_action_subject(
        "Проверить", "Проверка с документами", [], ""
    )
    assert action_two == "Проверить документы"
