from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _attachment_lines(result: str) -> list[str]:
    lines = result.split("\n")
    start = lines.index("") if "" in lines else len(lines)
    return [line for line in lines[start + 1 :] if " — " in line]


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

    lines = _attachment_lines(result)
    assert len(lines) == 4

    mapping = {line.split(" — ")[0]: line for line in lines}
    assert mapping.keys() == {att.filename for att in attachments}

    assert mapping["legacy.doc"].endswith("документ Word (текст недоступен)")
    assert mapping["old.xls"].endswith("таблица Excel (данные недоступны)")
    assert mapping["blank.docx"].endswith("текст не извлечён")
    assert "Дата | Сумма" in mapping["data.xlsx"]

    lower_result = result.lower()
    for forbidden in [
        "старый формат",
        "формат",
        "кодировка",
        "утилита",
        "не поддерживается",
        "по данным файла",
    ]:
        assert forbidden not in lower_result
