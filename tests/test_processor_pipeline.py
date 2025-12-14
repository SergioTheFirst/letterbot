from datetime import datetime
from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


def _processor() -> MessageProcessor:
    config = SimpleNamespace(llm_call=None)
    state = SimpleNamespace()
    return MessageProcessor(config=config, state=state)


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
    assert "<" not in result and ">" not in result
    lowered = result.lower()
    assert "html" not in lowered and "style" not in lowered and "table" not in lowered
    assert len(result.split("\n")) == 2


def test_image_attachments_are_ignored_completely():
    processor = _processor()
    att = Attachment(filename="photo.png", content=b"binary", content_type="image/png", text="Image text")
    msg = InboundMessage(subject="Pictures", body="Plain text message with info.", attachments=[att])

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "png" not in result.lower()
    assert len(result.split("\n")) == 2


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
    assert "contract.docx" in lines
    attachment_summary = lines[-1]
    assert len(attachment_summary) < len(long_text)
    assert "delivery schedules" not in attachment_summary


def test_domain_classification_logging_does_not_change_output(caplog):
    processor = _processor()
    msg = InboundMessage(
        subject="Invoice for services",
        body="Please pay invoice 123 by 12.12. Funds appreciated.",
        sender="billing@service.com",
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    expected_output = "🟡 ВАЖНО от Billing — Invoice for services (09:30)\nОплатить invoice for services"

    with caplog.at_level("INFO"):
        result = processor.process("billing@service.com", msg)

    assert result == expected_output
    assert "Domain detected: INVOICE" in caplog.text
