from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


FORBIDDEN_SUBSTRINGS = [
    "тело письма отсутств",
    "полезная информация",
    "не обнаруж",
    "не удалось",
    "недоступ",
    "текст не извлеч",
    "данные недоступ",
    "файл jpg без извлекаемого текста",
    "(≈",
    "записей)",
    "таблица:",
]


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, SimpleNamespace(save=lambda: None))


def test_message_has_no_negative_phrases():
    processor = _processor()
    msg = InboundMessage(
        subject="Пустое тело",
        sender="robot@example.com",
        body="",
        attachments=[
            Attachment(filename="draft.docx", content=b"", content_type="application/msword", text=""),
            Attachment(filename="table.xlsx", content=b"", content_type="application/vnd.ms-excel", text=""),
        ],
    )

    result = processor.process("user@example.com", msg) or ""
    lowered = result.lower()
    for phrase in FORBIDDEN_SUBSTRINGS:
        assert phrase not in lowered
