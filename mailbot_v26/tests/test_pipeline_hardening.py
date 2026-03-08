from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from mailbot_v26.llm.request_queue import LLMRequest
from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.pipeline.processor import (
    Attachment,
    InboundMessage,
    MessageProcessor,
    _collect_message_facts,
    _detect_attachment_doc_type,
    _normalize_subject_for_compare,
)


class _DummyState:
    def save(self) -> None:
        return None


def _processor() -> MessageProcessor:
    return MessageProcessor(SimpleNamespace(llm_call=None), _DummyState())


def test_no_mojibake_in_telegram_payload() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Invoice 42",
        sender="billing@example.com",
        body="Please review and pay the invoice.",
        attachments=[
            Attachment(
                filename="invoice.pdf",
                content=b"",
                content_type="application/pdf",
                text="Invoice amount due: 1200 USD",
            )
        ],
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    payload_text = processor.process("robot@example.com", msg)

    for token in ("вЂ", "РѕС‚", "вЂў", "РґРѕРіРѕРІ", "рџ"):
        assert token not in payload_text


def test_force_llm_always_bypasses_budget_gate(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_processor,
        "budget_gate",
        SimpleNamespace(can_use_llm=lambda _account_email: False),
    )

    can_use_llm, budget_gate_allow, override_applied = (
        pipeline_processor._resolve_llm_budget_access(
            account_email="acc@example.com",
            use_llm_candidate=True,
            force_llm_always=True,
            email_id=42,
        )
    )

    assert can_use_llm is True
    assert budget_gate_allow is False
    assert override_applied is True


def test_queue_timeout_uses_config_not_half_second(monkeypatch) -> None:
    calls: list[float] = []

    class _StubQueue:
        def enqueue(self, _request, *, timeout_sec: float) -> bool:
            calls.append(timeout_sec)
            return False

    monkeypatch.setattr(
        pipeline_processor,
        "get_llm_queue_config",
        lambda: SimpleNamespace(llm_request_queue_timeout_sec=123.0),
    )
    monkeypatch.setattr(
        pipeline_processor,
        "get_llm_request_queue",
        lambda: _StubQueue(),
    )

    queued = pipeline_processor._enqueue_llm_request_with_retry(
        LLMRequest(
            account_email="acc@example.com",
            email_id=1,
            subject="s",
            from_email="f@example.com",
            body_text="body",
            attachments=[],
            received_at=datetime.now(timezone.utc),
            input_chars=10,
        ),
        email_id=1,
    )

    assert queued is False
    assert calls == [123.0, 0.0]


def test_detect_attachment_doc_type_matches_russian_filename() -> None:
    assert (
        _detect_attachment_doc_type(
            filename="договор_поставки.docx", content_type="application/msword"
        )
        == "CONTRACT"
    )
    assert (
        _detect_attachment_doc_type(
            filename="счёт_77.pdf", content_type="application/pdf"
        )
        == "TABLE"
    )
    assert (
        _detect_attachment_doc_type(
            filename="счет_77.pdf", content_type="application/pdf"
        )
        == "TABLE"
    )
    assert (
        _detect_attachment_doc_type(
            filename="акт_сверки.pdf", content_type="application/pdf"
        )
        == "TABLE"
    )
    assert (
        _detect_attachment_doc_type(
            filename="накладная_1.pdf", content_type="application/pdf"
        )
        == "TABLE"
    )
    assert (
        _detect_attachment_doc_type(
            filename="расчетный_листок.pdf", content_type="application/pdf"
        )
        == "TABLE"
    )


def test_invoice_subject_detected_before_body() -> None:
    facts = _collect_message_facts(
        subject="Счёт №77",
        body_text="Please check details.",
        attachments=[],
        mail_type="UNKNOWN",
    )

    assert facts["invoice_signal"] is True
    assert facts["doc_kind"] == "invoice"


def test_thread_normalization_links_re_fw_subjects() -> None:
    assert _normalize_subject_for_compare("RE: FW: FWD: Invoice #77") == "invoice #77"
    assert _normalize_subject_for_compare("ОТВ: ПЕР: Счёт №77") == "счёт №77"


def test_payroll_never_invoice_even_with_amounts() -> None:
    facts = _collect_message_facts(
        subject="Расчетный листок",
        body_text="Начислено 150 000 руб. Удержано 13 000 руб. К выплате 137 000 руб.",
        attachments=[],
        mail_type="PAYROLL",
    )

    assert facts["doc_kind"] == "payroll"
    assert facts["invoice_signal"] is False
