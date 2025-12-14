from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


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

    lines = result.split("\n")
    assert lines[2].strip()

    blank_index = lines.index("")
    attachment_lines = [line for line in lines[blank_index + 1 :] if line.strip()]
    assert len(attachment_lines) == 2

    forbidden_phrases = {
        "документ содержит",
        "можно просмотреть",
        "без подробностей",
        "нужно изучить",
    }
    lowered = "\n".join(attachment_lines).lower()
    assert not any(phrase in lowered for phrase in forbidden_phrases)

    descriptions = [line.split(" — ", 1)[1] for line in attachment_lines]
    assert len(set(descriptions)) == 2

