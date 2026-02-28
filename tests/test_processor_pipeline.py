from datetime import datetime
import re
from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


def _processor() -> MessageProcessor:
    config = SimpleNamespace(llm_call=None)
    state = SimpleNamespace()
    return MessageProcessor(config=config, state=state)


def _attachment_lines(result: str, names: set[str]) -> list[str]:
    lines = [line for line in result.split("\n") if line.strip()]
    return [line for line in lines if any(line.startswith(name) for name in names)]


def test_html_body_is_sanitized_and_summarized():
    processor = _processor()
    body = """
    <html>
    <head><style>p {color:red;}</style></head>
    <body>
    <p>Hello <b>world</b>!</p>
    <table><tr><td>Important update</td></tr></table>
    </body>
    </html>
    """
    msg = InboundMessage(subject="Status", body=body, sender="robot@example.com")

    result = processor.process("robot@example.com", msg)

    assert result is not None
    cleaned = re.sub(r"</?[^>]+>", "", result)
    assert "<" not in cleaned and ">" not in cleaned
    lowered = cleaned.lower()
    assert "html" not in lowered and "style" not in lowered and "table" not in lowered
    assert len(result.split("\n")) >= 2


def test_image_attachments_are_ignored_completely():
    processor = _processor()
    att = Attachment(filename="photo.png", content=b"binary", content_type="image/png", text="Image text")
    msg = InboundMessage(subject="Pictures", body="Plain text message with info.", attachments=[att])

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "png" not in result.lower()
    assert len(result.split("\n")) >= 2


def test_attachment_is_summarized_not_dumped():
    processor = _processor()
    long_text = "This contract includes payment terms and delivery schedules. " * 5
    att = Attachment(
        filename="contract.docx",
        content=b"doc",
        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        text=long_text,
    )
    msg = InboundMessage(
        subject="Contract Update",
        body="Here is the latest agreement version with updates to review.",
        attachments=[att],
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    lines = result.split("\n")
    assert any("contract.docx" in line for line in lines)
    attachment_summary = next(line for line in lines if "contract.docx" in line)
    assert len(attachment_summary) < len(long_text)
    summary_text = attachment_summary.split(" — ", 1)[1]
    assert 2 <= len(summary_text.split()) <= 6


def test_domain_classification_logging_does_not_change_output(caplog):
    processor = _processor()
    msg = InboundMessage(
        subject="Invoice for services",
        body="Please pay invoice 123 by 12.12. Funds appreciated.",
        sender="billing@service.com",
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    with caplog.at_level("INFO"):
        result = processor.process("billing@service.com", msg)

    assert result is not None
    assert result.split("\n")[0].startswith("🟡 от Billing — Invoice for services")
    assert any(line.startswith("Оплатить") for line in result.split("\n"))
    assert "Domain detected" not in caplog.text


def test_primary_fact_and_attachments_compact_summaries():
    processor = _processor()
    attachments = [
        Attachment(
            filename="contract_v2.docx",
            content=b"doc",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Обновленная редакция договора. Требуется согласовать условия ЭДО до 15.03.",
        ),
        Attachment(
            filename="invoice.pdf",
            content=b"pdf",
            content_type="application/pdf",
            text="Счет за услуги связи на 45 000 ₽. Оплатить до 20.04.",
        ),
        Attachment(
            filename="price_list.xlsx",
            content=b"xls",
            content_type="application/vnd.ms-excel",
            text="Прайс-лист: обновлены цены на оборудование и сервис.",
        ),
        Attachment(
            filename="numbers.csv",
            content=b"csv",
            content_type="text/csv",
            text="Перечень объектов и номеров ____ телефонов.",
        ),
    ]

    msg = InboundMessage(
        subject="Новый договор и счета",
        body="Просим согласовать новый договор до 15.03. Аванс 120 000 ₽.",
        attachments=attachments,
    )

    result = processor.process("team@example.com", msg)

    assert result is not None
    lines = result.split("\n")

    summary_line = next(
        (
            line
            for line in lines
            if line.strip().startswith("<i>") and not line.startswith("<i>to:")
        ),
        "",
    )
    summary_text = re.sub(r"</?[^>]+>", "", summary_line).strip()
    assert 8 <= len(summary_text.split()) <= 12

    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)
    assert len(attachment_lines) == 4
    assert len({line.split(" — ")[0] for line in attachment_lines}) == 4
    assert "___" not in result
    assert "№" not in result


def test_all_document_attachments_render_even_without_text():
    processor = _processor()
    attachments = [
        Attachment(
            filename="draft.docx",
            content=b"docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="",
        ),
        Attachment(
            filename="legacy.doc",
            content=b"doc",
            content_type="application/msword",
            text=None,
        ),
        Attachment(
            filename="report.xlsx",
            content=b"xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Итоги квартала",
        ),
        Attachment(
            filename="table.xls",
            content=b"xls",
            content_type="application/vnd.ms-excel",
            text="",
        ),
    ]

    msg = InboundMessage(
        subject="Multiple docs",
        body="Формат А2 Формат А1 Формат А0 — это тело письма",
        attachments=attachments,
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    lines = result.split("\n")
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    assert len(attachment_lines) == 4
    assert all(name in result for name in ("draft.docx", "legacy.doc", "report.xlsx", "table.xls"))
    assert "формат а2" not in " ".join(attachment_lines).lower()
    assert "по данным файла" not in result.lower()
    assert any(line.startswith("draft.docx") for line in attachment_lines)
    assert any(line.startswith("legacy.doc") for line in attachment_lines)
    assert any(line.startswith("table.xls") for line in attachment_lines)
    assert all("таблица:" not in line for line in attachment_lines)
