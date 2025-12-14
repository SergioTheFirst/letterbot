from datetime import datetime
import re
from types import SimpleNamespace

from mailbot_v26.pipeline import processor
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:
        return None


def test_message_processor_formats_output(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Краткое резюме письма."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return f"Сводка вложения {kind}."

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    msg = InboundMessage(
        subject="Subject line",
        sender="sender@example.com",
        body="body text",
        attachments=[
            Attachment(
                filename="file.pdf",
                content=b"data",
                text="Содержимое файла содержит несколько предложений, которые нужно изложить кратко и аккуратно для передачи в Телеграм. Здесь достаточно текста, чтобы сформировать устойчивое резюме.",
            )
        ],
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    lines = output.split("\n")
    assert lines[0].startswith("09:30 01.01.2024")
    assert lines[1] == "sender@example.com"
    assert lines[2] == "Subject line"
    assert "Письмо от sender@example.com" in output
    assert "file.pdf" in output


def test_message_processor_handles_empty(monkeypatch):
    monkeypatch.setattr(
        processor,
        "LLMSummarizer",
        lambda cfg: SimpleNamespace(
            summarize_email=lambda text: "",
            summarize_attachment=lambda text, kind="PDF": "",
        ),
    )

    cfg = SimpleNamespace(llm_call=None)
    msg = InboundMessage(subject="", sender="", body="", attachments=[])
    output = MessageProcessor(cfg, DummyState()).process("account", msg)
    assert output is not None
    lines = [line for line in output.split("\n") if line.strip()]
    assert len(lines) >= 4


def test_message_processor_strips_forwarded_headers(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return text

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return text

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    body = "Текст сообщения\nFrom: other@example.com\nSent: now\nSubject: test"
    msg = InboundMessage(
        subject="Subj",
        sender="sender@example.com",
        body=body,
        attachments=[],
        received_at=datetime(2024, 1, 1, 10, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "From:" not in output
    assert "Sent:" not in output


def test_message_processor_caps_length(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "A" * 4000

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    msg = InboundMessage(
        subject="Subj",
        sender="sender@example.com",
        body="text",
        attachments=[],
        received_at=datetime(2024, 1, 1, 11, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert len(output) <= 3500


def test_processor_generates_fallback_summary(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "   "

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    body = "Здравствуйте! Это длинное письмо с подробностями. Оно содержит несколько предложений. Спасибо."
    msg = InboundMessage(
        subject="Subj",
        sender="sender@example.com",
        body=body,
        attachments=[],
        received_at=datetime(2024, 2, 2, 12, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "длинное письмо" in output


def test_processor_attachment_empty_text_handled(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Краткое резюме тела письма"

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    body = "Основное содержимое письма. Оно довольно длинное и информативное."
    attachments = [Attachment(filename="table.xlsx", content=b"data", text="")]
    msg = InboundMessage(
        subject="Subj",
        sender="sender@example.com",
        body=body,
        attachments=attachments,
        received_at=datetime(2024, 3, 3, 13, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "Основное содержимое" in output
    assert "xlsx" not in output
    assert "таблица" not in output.lower()


def test_processor_never_header_only(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return ""

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    msg = InboundMessage(
        subject="",
        sender="",
        body="",
        attachments=[],
        received_at=datetime(2024, 4, 4, 14, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    lines = [line for line in output.split("\n") if line.strip()]
    assert len(lines) >= 4


def test_processor_output_no_binary(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return text

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return text

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    body = (
        "Main text\n"
        "From: forwarded@example.com\n"
        "Sent: yesterday\n"
        "Subject: forwarded\n"
        "IDAT should go away"
    )
    attachments = [
            Attachment(filename="image.png", content=b"data", text="IHDR bad"),
            Attachment(filename="file.docx", content=b"data", text="Useful text"),
            Attachment(filename="archive.zip", content=b"data", text="PK header"),
        ]
    msg = InboundMessage(
        subject="Subj",
        sender="sender@example.com",
        body=body,
        attachments=attachments,
        received_at=datetime(2024, 1, 1, 12, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "IHDR" not in output
    assert "PK" not in output
    assert "IDAT" not in output
    assert "image.png" not in output.lower()
    assert "archive.zip" not in output.lower()
    assert "Useful text" not in output
    assert "file.docx" not in output


def test_processor_strips_encoded_headers(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Обработанное письмо содержит полезную информацию. Оно включает пояснение."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    body = "Короткое сообщение"
    attachments = []
    msg = InboundMessage(
        subject="=?koi8-r?B?5NLJ?=",  # encoded garbage should be removed
        sender="=?utf-8?B?0J3QtdC80LXQ?=",
        body=body,
        attachments=attachments,
        received_at=datetime(2024, 5, 5, 15, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "=?" not in output
    assert len([line for line in output.split("\n") if line.strip()]) >= 3


def test_html_body_clean_summary(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Письмо о встрече. Договорились обсудить детали проекта."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return ""

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    html_body = "<html><body><h1>Привет</h1><p>Встреча завтра</p><style>.x{}</style></body></html>"
    msg = InboundMessage(
        subject="HTML письмо",
        sender="sender@example.com",
        body=html_body,
        attachments=[],
        received_at=datetime(2024, 6, 6, 16, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "<" not in output and ">" not in output
    assert "html" not in output.lower()


def test_image_attachments_silenced(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Краткое резюме письма."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return "Содержание документа изложено кратко."

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    attachments = [
            Attachment(filename="photo.jpg", content=b"data", text=""),
            Attachment(
                filename="report.pdf",
                content=b"data",
                text="Отчет по проекту содержит разделы о задачах, сроках и результатах. Текст достаточно длинный, чтобы сделать выводы.",
            ),
        ]
    msg = InboundMessage(
        subject="Тема",
        sender="sender@example.com",
        body="Сообщение",
        attachments=attachments,
        received_at=datetime(2024, 6, 7, 17, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "photo.jpg" not in output.lower()
    assert "report.pdf" in output


def test_raw_attachment_text_blocked(monkeypatch):
    raw_text = "Конфиденциальный отчет о продажах за квартал. Включает суммы и детали." * 2

    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Сообщение содержит вложение."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return raw_text

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    attachments = [Attachment(filename="report.docx", content=b"data", text=raw_text)]
    msg = InboundMessage(
        subject="Отчет",
        sender="sender@example.com",
        body="Основное сообщение",
        attachments=attachments,
        received_at=datetime(2024, 6, 8, 18, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    assert "report.docx" in output
    assert raw_text[:40] not in output


def test_attachment_summary_sentence_limit(monkeypatch):
    class DummySummarizer:
        def __init__(self, _):
            pass

        def summarize_email(self, text: str) -> str:
            return "Сообщение об обновлении."

        def summarize_attachment(self, text: str, kind: str = "PDF") -> str:
            return "Первое. Второе. Третье. Четвертое. Пятое."

    monkeypatch.setattr(processor, "LLMSummarizer", DummySummarizer)

    cfg = SimpleNamespace(llm_call=lambda x: "ok")
    attachments = [Attachment(filename="summary.pdf", content=b"data", text="Некоторый текст" * 10)]
    msg = InboundMessage(
        subject="Обновление",
        sender="sender@example.com",
        body="Основной текст",
        attachments=attachments,
        received_at=datetime(2024, 6, 9, 19, 0),
    )

    output = MessageProcessor(cfg, DummyState()).process("login", msg)
    assert output is not None
    lines = output.split("\n")
    idx = lines.index("summary.pdf")
    summary_text = lines[idx + 1]
    sentences = re.findall(r"[^.!?]+[.!?]", summary_text)
    assert len(sentences) <= 3
