from types import SimpleNamespace
import re

from mailbot_v26.pipeline.processor import Attachment, AttachmentSummary, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def _strip_html(line: str) -> str:
    return re.sub(r"</?[^>]+>", "", line)


def _attachment_lines(result: str, names: set[str]) -> list[str]:
    lines = [_strip_html(line) for line in result.split("\n") if line.strip()]
    return [line for line in lines if any(line.startswith(name) for name in names)]


def test_attachment_snippet_trimming_uses_expanded_limit() -> None:
    old_limit = 60
    new_limit = MessageProcessor._ATTACHMENT_SNIPPET_LIMIT

    assert new_limit == old_limit * 2

    long_text = "учреждение-" * 15
    trimmed = MessageProcessor._trim_attachment_snippet(long_text)

    assert len(trimmed) == new_limit
    assert trimmed.endswith("…")
    assert trimmed[:-1] == long_text[: new_limit - 1]


def test_message_formatter_respects_expanded_attachment_snippet() -> None:
    processor = _processor()

    long_summary = "учреждения и подразделения представлены очень подробно " * 10
    attachment = AttachmentSummary(
        filename="a.pdf",
        description=long_summary,
        kind="PDF",
        priority=0,
        text_length=len(long_summary),
    )

    rendered = processor._render_attachments([attachment])

    assert rendered

    line = _attachment_lines("\n".join(rendered), {attachment.filename})[0]
    snippet = line.split(" — ", 1)[1]

    assert len(snippet) <= MessageProcessor._ATTACHMENT_SNIPPET_LIMIT
    assert len(snippet) > 60
