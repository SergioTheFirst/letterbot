from types import SimpleNamespace

import pytest

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


@pytest.mark.parametrize("include_image", [True, False])
def test_renders_all_non_image_attachments_even_if_extraction_fails(monkeypatch, include_image):
    processor = _processor()

    outcomes: list[object] = [
        ("", 0),
        ("Краткая сводка", 5),
        ("Код, Наименование", 8),
        Exception("boom"),
    ]

    def fake_summarize(self, att, subject, kind):
        result = outcomes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(MessageProcessor, "_summarize_attachment", fake_summarize)

    attachments = [
        Attachment(filename="report.doc", content=b"1", content_type="application/msword", text=""),
        Attachment(
            filename="note.docx",
            content=b"22",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Небольшая заметка",
        ),
        Attachment(
            filename="table.xlsx",
            content=b"333",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Код; Наименование; Цена",
        ),
        Attachment(
            filename="table.xlsx",
            content=b"4444",
            content_type="application/vnd.ms-excel",
            text="Ещё одна таблица",
        ),
    ]

    if include_image:
        attachments.append(Attachment(filename="image.png", content=b"", content_type="image/png", text=""))

    msg = InboundMessage(
        subject="Комплект файлов",
        sender="ops@example.com",
        body="Прикладываем документы",
        attachments=attachments,
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = result.split("\n")
    blank_index = lines.index("") if "" in lines else len(lines)
    attachment_lines = [line for line in lines[blank_index + 1 :] if line.strip()]

    assert len(attachment_lines) == 4
    for name in ["report.doc", "note.docx", "table.xlsx"]:
        assert sum(line.startswith(name) for line in attachment_lines) >= 1

    table_lines = [line for line in attachment_lines if line.startswith("table.xlsx")]
    assert len(table_lines) == 2

    assert any(line.endswith("по данным файла") for line in attachment_lines)
    assert not any("image.png" in line for line in attachment_lines)
