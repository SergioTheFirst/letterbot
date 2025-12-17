from types import SimpleNamespace

from mailbot_v26.pipeline.processor import InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def test_footer_contains_account_login_at_the_end():
    processor = _processor()
    message = InboundMessage(subject="Test", body="Body")

    result = processor.process("user@example.com", message)

    assert result is not None
    lines = [line for line in result.split("\n") if line.strip()]
    assert lines[-1] == "<i>to: user@example.com</i>"
