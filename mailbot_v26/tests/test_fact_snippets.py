from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime

from mailbot_v26.domain.fact_snippets import (
    pick_attachment_fact,
    pick_email_body_fact,
)
from mailbot_v26.pipeline.processor import Attachment, InboundMessage, MessageProcessor


class DummyState:
    def save(self) -> None:  # pragma: no cover - placeholder
        return None


def _processor() -> MessageProcessor:
    cfg = SimpleNamespace(llm_call=None)
    return MessageProcessor(cfg, DummyState())


def test_email_body_fact_with_amount_and_month():
    text = """Добрый день
    сумма 9 159,43 ₽ период декабрь 2025, просьба оплатить."""

    fact = pick_email_body_fact(text)

    assert fact is not None
    assert "₽" in fact
    assert "декабрь" in fact.lower()


def test_contract_attachment_snippet_contains_party_tokens():
    text = """СОГЛАШЕНИЕ
    ОАО «КАРАВАЙ» выступает как Поставщик по договору поставки."""

    snippet = pick_attachment_fact(text, "contract.docx", "CONTRACT")

    assert snippet is not None
    assert any(token in snippet for token in ["ОАО", "Поставщик"])


def test_excel_headers_preserved_in_snippet():
    text = """Код | Наименование | Цена
    123 | Тестовый товар | 1200"""

    snippet = pick_attachment_fact(text, "prices.xlsx", "TABLE")

    assert snippet == "Код, Наименование, Цена"


def test_no_generic_filler_produced():
    snippet = pick_attachment_fact("Стоимость указана", "file.doc", "OTHER")
    body_fact = pick_email_body_fact("Привет, это просто письмо без цифр.")

    combined = " ".join(filter(None, [snippet, body_fact]))

    assert "новый документ" not in combined.lower()


def test_attachments_without_snippet_use_fallback():
    processor = _processor()
    msg = InboundMessage(
        subject="Пустое вложение",
        sender="user@example.com",
        body="Сообщение без фактов",
        attachments=[Attachment(filename="blank.pdf", content=b"data", content_type="application/pdf", text="")],
        received_at=datetime(2024, 1, 1, 9, 0),
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "blank.pdf — по данным файла" in result


__all__ = []
