from types import SimpleNamespace

import pytest

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


@pytest.mark.parametrize("include_image", [True, False])
def test_renders_all_non_image_attachments_even_if_extraction_fails(monkeypatch, include_image):
    # Force the class to drop items when the cap is applied, so we can assert the fix explicitly.
    monkeypatch.setattr(MessageProcessor, "_MAX_ATTACHMENTS", 3)

    processor = _processor()

    outcomes: list[object] = [
        Exception("boom"),
        ("", 0),
        ("Короткий текст", 5),
    ]

    def fake_summarize(self, att, subject, kind):
        result = outcomes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(MessageProcessor, "_summarize_attachment", fake_summarize)

    attachments = [
        Attachment(filename="a.doc", content=b"1", content_type="application/msword", text=""),
        Attachment(
            filename="b.docx",
            content=b"22",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Небольшая заметка",
        ),
        Attachment(
            filename="c.xlsx",
            content=b"333",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Код; Наименование; Цена",
        ),
        Attachment(
            filename="d.xlsx",
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

    attachment_lines = _attachment_lines(result)
    attachment_entries = [line for line in attachment_lines if not line.startswith("ещё ")]
    total_non_image = 4
    expected_count = total_non_image

    assert len(attachment_entries) == expected_count
    expected = {"a.doc", "b.docx", "c.xlsx", "d.xlsx"}
    rendered_files = {line.split(" — ")[0] if " — " in line else line for line in attachment_entries}
    assert expected == rendered_files

    assert "ещё" not in "\n".join(attachment_lines)
    assert "по данным файла" not in "\n".join(attachment_lines)
