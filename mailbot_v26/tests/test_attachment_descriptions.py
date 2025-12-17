from types import SimpleNamespace
import re

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _strip_html(line: str) -> str:
    return re.sub(r"</?[^>]+>", "", line)


def _attachment_lines(result: str, names: set[str]) -> list[str]:
    lines = [_strip_html(line) for line in result.split("\n") if line.strip()]
    return [line for line in lines if any(line.startswith(name) for name in names)]


def test_office_attachment_descriptions_follow_rules() -> None:
    processor = _processor()

    attachments = [
        Attachment(
            filename="blank.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="",
        ),
        Attachment(
            filename="legacy.doc",
            content=b"",
            content_type="application/msword",
            text="Старый документ с обложкой",
        ),
        Attachment(
            filename="data.xlsx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Дата | Сумма\n10.10 | 1000",
        ),
        Attachment(
            filename="old.xls",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="Любые ячейки",
        ),
    ]

    msg = InboundMessage(
        subject="Комплект вложений",
        sender="sender@example.com",
        body="Тестовое сообщение",
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)
    assert result is not None

    names = {att.filename for att in attachments}
    lines = _attachment_lines(result, names)
    assert len(lines) == 4

    mapping: dict[str, str] = {}
    for line in lines:
        if " — " in line:
            name, desc = line.split(" — ", 1)
        else:
            name, desc = line, ""
        mapping[name] = desc
    assert mapping.keys() == {att.filename for att in attachments}

    assert mapping["legacy.doc"]
    assert mapping["blank.docx"] == ""

    for excel_name in ("data.xlsx", "old.xls"):
        assert "таблица:" not in mapping[excel_name]
        assert "|" not in mapping[excel_name]

    assert "Дата | Сумма" not in mapping["data.xlsx"]

    lower_result = result.lower()
    for forbidden in ["недоступ", "не извлеч", "таблица:"]:
        assert forbidden not in lower_result


def test_excel_and_docx_summaries_are_compact() -> None:
    processor = _processor()

    attachments = [
        Attachment(
            filename="прайс_на_оборудование.xlsx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text=(
                "Код;Наименование;Цена;Количество;Итог;Ставка НДС\n"
                "123;Станок;100000;2;200000;20%\n"
                "987;Запчасть;5000;10;50000;20%"
            ),
        ),
        Attachment(
            filename="длинный_отчёт.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="""В этом отчете перечислены основные результаты проверки оборудования на заводе.""",
        ),
    ]

    msg = InboundMessage(
        subject="Компактные описания",
        sender="sender@example.com",
        body="Смотрите вложения",
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)
    assert result is not None

    names = {att.filename for att in attachments}
    lines = _attachment_lines(result, names)
    mapping = {line.split(" — ")[0]: line for line in lines}

    excel_line = mapping["прайс_на_оборудование.xlsx"]
    assert "таблица:" not in excel_line
    assert ";" not in excel_line
    assert len(excel_line) <= 120

    docx_summary = mapping["длинный_отчёт.docx"].split(" — ", 1)[1]
    assert len(docx_summary.split()) <= 12
    assert "\"" not in docx_summary
