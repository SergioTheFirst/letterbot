import re
from types import SimpleNamespace

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, SimpleNamespace(save=lambda: None))


def _summary_line(result: str) -> str:
    for line in result.split("\n"):
        if line.strip().startswith("<i>") and not line.startswith("<i>to:"):
            return re.sub(r"</?[^>]+>", "", line)
    return ""


def _word_count(text: str) -> int:
    return len([word for word in text.split() if word.strip()])


def test_html_body_becomes_neutral_summary():
    processor = _processor()
    msg = InboundMessage(
        subject="Отчет", 
        sender="team@example.com",
        body=(
            "<html><body><p>Здравствуйте!</p><div>Прикрепляем отчет за май." 
            "</div><div>Срок сдачи до 10.06. </div><div>С уважением, Иван</div></body></html>"
        ),
    )

    result = processor.process("user@example.com", msg)
    summary = _summary_line(result or "")

    assert "<" not in summary
    assert 8 <= _word_count(summary) <= 12
    assert "отчет" in summary.lower()


def test_empty_body_uses_fallback_phrase():
    processor = _processor()
    msg = InboundMessage(subject="Без тела", sender="noreply@example.com", body="")

    result = processor.process("user@example.com", msg)
    summary = _summary_line(result or "")

    assert summary == ""
    assert len([line for line in result.split("\n") if line.strip()]) == 4


def test_long_body_trims_to_word_budget():
    processor = _processor()
    long_body = " ".join(["Подробно" for _ in range(50)])
    msg = InboundMessage(subject="Длинное письмо", sender="ops@example.com", body=long_body)

    result = processor.process("user@example.com", msg)
    summary = _summary_line(result or "")

    assert 8 <= _word_count(summary) <= 12
    assert len(summary) <= 120


def test_telegram_preview_stays_compact():
    processor = _processor()
    msg = InboundMessage(
        subject="Информация",
        sender="info@example.com",
        body="Сообщаем об изменении расписания, просьба проверить детали.",
        attachments=[Attachment(filename="note.txt", content=b"", text="")],
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    lines = [line for line in result.split("\n") if line.strip()]

    summary = _summary_line(result)
    assert 8 <= _word_count(summary) <= 12
    assert len(summary) <= 120
    assert len(lines) <= 7


def test_greeting_only_body_skipped():
    processor = _processor()
    msg = InboundMessage(
        subject="Приветствие",
        sender="hello@example.com",
        body="Здравствуйте,\n\n--\nС уважением",
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = [line for line in result.split("\n") if line.strip()]
    assert len(lines) == 4
    assert all("здравствуйте" not in line.lower() for line in lines)
