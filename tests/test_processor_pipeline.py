from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


def _processor():
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
    body_section = result.split("\n\n", 1)[1]
    sentences = [s for s in body_section.split(".") if s.strip()]
    assert len(sentences) >= 2


def test_image_attachments_are_ignored_completely():
    processor = _processor()
    att = Attachment(filename="photo.png", content=b"binary", content_type="image/png", text="Image text")
    msg = InboundMessage(subject="Pictures", body="Plain text message with info.", attachments=[att])

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "png" not in result.lower()
    assert "photo" not in result.lower()


def test_short_attachment_stays_silent():
    processor = _processor()
    att = Attachment(
        filename="brief.pdf",
        content=b"pdf",
        content_type="application/pdf",
        text="Too short",
    )
    msg = InboundMessage(subject="Docs", body="Body content with enough detail to be summarized properly.", attachments=[att])

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "brief.pdf" not in result


def test_attachment_is_summarized_not_dumped():
    processor = _processor()
    long_text = "This contract includes payment terms and delivery schedules. " * 10
    unique_phrase = "delivery schedules"
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
    assert "contract.docx" in result
    attachment_section = result.split("contract.docx")[-1]
    assert unique_phrase not in attachment_section
    assert len(attachment_section.split(".")) >= 2


def test_body_summary_contains_action_and_keyword():
    processor = _processor()
    body = "Прошу согласовать кооперацию по поставке кабеля и подтвердить сроки отправки."
    msg = InboundMessage(subject="Cooperation", body=body, sender="team@example.com")

    result = processor.process("user@example.com", msg)

    assert result is not None
    lower = result.lower()
    assert "прошу" in lower
    assert "кооперацию" in lower


def test_forbidden_templates_are_blocked():
    processor = _processor()
    body = "Краткое уведомление: направляю договор на подпись."
    msg = InboundMessage(subject="Doc", body=body, sender="user@example.com")

    result = processor.process("user@example.com", msg)

    forbidden = [
        "касается темы",
        "по теме письма",
        "автор прислал краткое сообщение",
        "без подробностей",
        "без технических деталей",
        "можно просмотреть при необходимости",
        "файл дополняет информацию",
    ]
    normalized = result.lower()
    assert all(pattern not in normalized for pattern in forbidden)


def test_excel_attachment_mentions_pricing():
    processor = _processor()
    att_text = "Прайс-лист: цены на кабель и крепеж указаны в таблице."
    att = Attachment(
        filename="prices.xlsx",
        content=b"xls",
        content_type="application/vnd.ms-excel",
        text=att_text,
    )
    msg = InboundMessage(
        subject="Pricing",
        body="Отправляю обновленный прайс со стоимостью материалов.",
        attachments=[att],
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    section = result.split("prices.xlsx")[-1].lower()
    assert "прайс" in section or "цена" in section


def test_contract_attachment_mentions_agreement():
    processor = _processor()
    att_text = "Договор поставки между сторонами, включает обязательства и сроки." * 3
    att = Attachment(
        filename="agreement.docx",
        content=b"doc",
        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        text=att_text,
    )
    msg = InboundMessage(subject="Agreement", body="Прикладываю текст договора.", attachments=[att])

    result = processor.process("user@example.com", msg)

    assert result is not None
    section = result.split("agreement.docx")[-1].lower()
    assert "договор" in section or "соглашение" in section

