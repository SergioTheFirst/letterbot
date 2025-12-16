import importlib
import logging

import pytest

from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder state
        return None


def _processor() -> MessageProcessor:
    return MessageProcessor(config=type("Cfg", (), {"llm_call": None})(), state=DummyState())


def test_no_domain_logs_are_emitted(caplog):
    processor = _processor()
    msg = InboundMessage(
        subject="Invoice attached",
        sender="billing@example.com",
        body="Счет на оплату во вложении",
        attachments=[Attachment(filename="invoice.pdf", content=b"", content_type="application/pdf", text="")],
    )

    with caplog.at_level(logging.INFO, logger="mailbot_v26.pipeline.processor"):
        result = processor.process("robot@example.com", msg)

    assert result
    assert not any(
        "Domain detected" in record.message or "Domain priority suggestion" in record.message
        for record in caplog.records
    )


def test_domain_detector_symbols_absent():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("mailbot_v26.domain.domain_classifier")

    domain_module = importlib.import_module("mailbot_v26.domain")
    assert not hasattr(domain_module, "DomainClassifier")
