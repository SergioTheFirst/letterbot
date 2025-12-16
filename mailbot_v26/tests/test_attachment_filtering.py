from email import message_from_bytes
from types import SimpleNamespace

import mailbot_v26.start as start
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _attachment_lines(result: str) -> list[str]:
    lines = result.split("\n")
    blank_index = lines.index("") if "" in lines else len(lines)
    return [line for line in lines[blank_index + 1 :] if line.strip()]


def test_no_attachment_bin_from_inline_parts():
    raw_email = b"".join(
        [
            b"MIME-Version: 1.0\r\n",
            b"Content-Type: multipart/mixed; boundary=xyz\r\n\r\n",
            b"--xyz\r\n",
            b"Content-Type: text/plain; charset=utf-8\r\n\r\n",
            b"Hello text body\r\n",
            b"--xyz\r\n",
            b"Content-Type: text/html; charset=utf-8\r\n\r\n",
            b"<html><head><style>.cls{color:red;}</style></head><body>Hi</body></html>\r\n",
            b"--xyz\r\n",
            b"Content-Type: text/css; charset=utf-8\r\n\r\n",
            b"body { background: #fff; }\r\n",
            b"--xyz--\r\n",
        ]
    )

    email_obj = message_from_bytes(raw_email)
    attachments = start._extract_attachments(email_obj, 10)
    body = start._extract_body(email_obj)

    assert attachments == []

    processor = _processor()
    msg = InboundMessage(subject="Inline", sender="user@example.com", body=body, attachments=attachments)

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "attachment.bin" not in result


def test_no_placeholder_po_dannym_faila():
    processor = _processor()
    attachments = [
        Attachment(
            filename="empty.docx",
            content=b"zeros",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="",
        )
    ]
    msg = InboundMessage(
        subject="Пустое вложение",
        sender="ops@example.com",
        body="Тело письма",
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)

    assert result is not None
    assert "по данным файла" not in result
    lines = _attachment_lines(result)
    assert any(line.startswith("empty.docx") for line in lines)


def test_ignores_images_and_fonts():
    processor = _processor()
    msg = InboundMessage(
        subject="Ресурсы",
        sender="ops@example.com",
        body="Смотрите вложения",
        attachments=[
            Attachment(filename="photo.png", content=b"img", content_type="image/png", text=""),
            Attachment(filename="font.woff", content=b"font", content_type="font/woff", text=""),
            Attachment(
                filename="report.docx",
                content=b"data",
                content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                text="""
                Отчет по проекту с основными показателями, сроками реализации и ответственными лицами.
                """,
            ),
        ],
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    attachment_lines = _attachment_lines(result)
    assert any(line.startswith("report.docx") for line in attachment_lines)
    assert not any("photo.png" in line for line in attachment_lines)
    assert any("font.woff" in line for line in attachment_lines)


def test_keeps_real_office_attachments():
    processor = _processor()
    msg = InboundMessage(
        subject="Документы",
        sender="ops@example.com",
        body="Комплект файлов",
        attachments=[
            Attachment(
                filename="terms.docx",
                content=b"doc",
                content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                text="""
                Договор поставки оборудования включает сроки, стоимость и ответственность сторон за исполнение обязательств.
                """,
            ),
            Attachment(
                filename="schedule.xlsx",
                content=b"sheet",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                text="""
                Период | Объем | Цена
                Январь | 10 | 1000
                Февраль | 20 | 2000
                """,
            ),
        ],
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    attachment_lines = _attachment_lines(result)
    assert len(attachment_lines) == 2
    assert any(line.startswith("terms.docx") for line in attachment_lines)
    assert any(line.startswith("schedule.xlsx") for line in attachment_lines)

