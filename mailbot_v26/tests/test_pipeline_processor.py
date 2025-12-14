from datetime import datetime
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
        attachments=[Attachment(filename="file.pdf", content=b"data", text="content")],
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
    assert "PDF" in output


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
    assert "table.xlsx" in output
    assert "таблица" in output.lower() or "файл" in output.lower()


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
    assert "Useful text" in output


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
