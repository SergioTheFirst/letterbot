from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _attachment_lines(result: str) -> list[str]:
    lines = result.split("\n")
    start = lines.index("") if "" in lines else len(lines)
    return [
        line
        for line in lines[start + 1 :]
        if line.strip() and not line.startswith("📎") and not line.startswith("📂") and not line.startswith("ещё ")
    ]


def test_multiple_attachments_all_processed() -> None:
    processor = _processor()

    attachments = [
        Attachment(
            filename="proposal.doc",
            content=b"doc",
            content_type="application/msword",
            text="Commercial proposal with pricing and delivery terms.",
        ),
        Attachment(
            filename="summary.docx",
            content=b"docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Updated contract summary for review and approval.",
        ),
        Attachment(
            filename="report.xlsx",
            content=b"xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Quarter | Revenue | Expenses\nQ1 | 100 | 50\nQ2 | 150 | 60",
        ),
        Attachment(
            filename="budget.xls",
            content=b"xls",
            content_type="application/vnd.ms-excel",
            text="Month | Plan | Fact\nJan | 10 | 12\nFeb | 11 | 10",
        ),
    ]

    msg = InboundMessage(
        subject="Several attachments",
        sender="ops@example.com",
        body="Please review all documents",
        attachments=attachments,
    )

    result = processor.process("ops@example.com", msg)

    assert result is not None
    attachment_lines = _attachment_lines(result)

    for name in {att.filename for att in attachments}:
        assert any(line.startswith(name) for line in attachment_lines), f"Missing {name}"

    assert len(attachment_lines) == len(attachments)


def test_main_attachment_block_and_clean_lines() -> None:
    processor = _processor()

    attachments = [
        Attachment(
            filename="primary.docx",
            content=b"docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="",
        ),
        Attachment(
            filename="legacy.doc",
            content=b"doc",
            content_type="application/msword",
            text="",
        ),
        Attachment(
            filename="table.xlsx",
            content=b"xlsx",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Наименование | Кол-во\nСтул | 4\nСтол | 2",
        ),
        Attachment(
            filename="old.xls",
            content=b"xls",
            content_type="application/vnd.ms-excel",
            text="",
        ),
    ]

    msg = InboundMessage(
        subject="Check attachments",
        sender="ops@example.com",
        body="Attachments included",
        attachments=attachments,
    )

    result = processor.process("ops@example.com", msg)
    assert result is not None

    lower_result = result.lower()
    assert "📎 Главное вложение:" in result
    assert "📂 Остальные вложения (3):" in result
    assert "старый формат" not in lower_result
    assert "attachment.bin" not in lower_result
    assert "=?koi8-r?" not in lower_result
    assert "формат" not in lower_result

    for name in {att.filename for att in attachments}:
        assert name in result

