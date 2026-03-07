from types import SimpleNamespace
import re

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


def test_body_and_attachments_rendered_with_summaries():
    processor = _processor()
    body = (
        "Здравствуйте!\n\n"
        "Предоставляем обновленный график поставок на май и просим подтвердить сроки отгрузки.\n\n"
        "С уважением, отдел снабжения"
    )

    attachments = [
        Attachment(
            filename="agreement.doc",
            content=b"",
            content_type="application/msword",
            text=(
                "Договор поставки продукции между ООО КАРАВАЙ и ООО ТОРГОВЫЙ ДОМ. "
                "Условия оплаты по безналичному расчету, срок действия до 12.12.2024."
            ),
        ),
        Attachment(
            filename="note.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Краткая памятка по проекту",
        ),
        Attachment(
            filename="prices.xlsx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text=(
                "Услуга | Тариф | Номер\n"
                "Телефония | 500 | 8800\n"
                "Поддержка | 300 | 8811\n"
                "Обслуживание | 200 | 8822"
            ),
        ),
        Attachment(
            filename="empty.xlsx",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="",
        ),
    ]

    msg = InboundMessage(
        subject="График поставок и прайс",
        sender="manager@example.com",
        body=body,
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)
    assert result is not None

    lines = [line for line in result.split("\n") if line.strip()]
    assert lines[2].strip()

    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)
    assert len(attachment_lines) == 4

    for filename in ["agreement.doc", "note.docx", "prices.xlsx", "empty.xlsx"]:
        assert any(line.startswith(filename) for line in attachment_lines)


def test_multiple_attachments_all_processed():
    processor = _processor()

    attachments = [
        Attachment(
            filename="report.doc",
            content=b"doc",
            content_type="application/msword",
            text="Отчет по проекту с ключевыми выводами и дедлайнами.",
        ),
        Attachment(
            filename="plan.docx",
            content=b"docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="План работ и согласованные этапы выполнения.",
        ),
        Attachment(
            filename="sales.xlsx",
            content=b"xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Месяц | Продажи | Маржа\nЯнварь | 120 | 40\nФевраль | 150 | 50",
        ),
        Attachment(
            filename="legacy.xls",
            content=b"xls",
            content_type="application/vnd.ms-excel",
            text="Старый формат таблицы с итогами квартала.",
        ),
    ]

    msg = InboundMessage(
        subject="Комплект документов",
        sender="ops@example.com",
        body="Смотрите все вложения",
        attachments=attachments,
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = result.split("\n")
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    expected = {"report.doc", "plan.docx", "sales.xlsx", "legacy.xls"}
    rendered = {line.split(" — ")[0] if " — " in line else line for line in attachment_lines}
    assert expected == rendered
