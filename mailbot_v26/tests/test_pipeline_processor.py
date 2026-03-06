from datetime import datetime, timedelta, timezone
import sqlite3
import re
from types import SimpleNamespace

from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.pipeline.processor import (
    Attachment,
    InboundMessage,
    MessageInterpretation,
    MessageProcessor,
    PriorityResultV2,
    _apply_sender_memory_to_priority,
    _build_heuristic_attachment_summaries,
    _build_priority_signal_text,
    _build_message_decision,
    _build_document_identity,
    _build_telegram_text,
    _build_sender_relationship_profile,
    _build_heuristic_summary,
    _consistency_check_message_facts,
    _collect_message_facts,
    _detect_conversation_context,
    _maybe_drop_duplicate_subject_line,
    _soften_duplicate_action,
    _score_message_facts,
    _validate_message_facts,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


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


def test_bank_invoice_marked_red():
    processor = _processor()
    msg = InboundMessage(
        subject="Счет на оплату услуг",
        sender="billing@bank.ru",
        body="Просим срочно оплатить счет до завтра, сумма 12000 руб.",
        attachments=[Attachment(filename="invoice.pdf", content=b"", content_type="application/pdf", text="Счет на оплату")],
        received_at=datetime(2024, 1, 1, 9, 30),
    )

    result = processor.process("robot@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔴 от")


def test_contract_approval_marked_yellow():
    processor = _processor()
    msg = InboundMessage(
        subject="Согласование договора поставки",
        sender="manager@client.com",
        body="Просьба согласовать договор и вернуть подписанный экземпляр.",
        attachments=[Attachment(filename="contract.docx", content=b"", content_type="application/msword", text="Условия договора")],
        received_at=datetime(2024, 2, 2, 10, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🟡 от")


def test_hr_policy_info_blue():
    processor = _processor()
    msg = InboundMessage(
        subject="Обновление HR политики",
        sender="hr@company.com",
        body="Подготовили обновление корпоративной политики, ознакомьтесь на портале.",
        attachments=[],
        received_at=datetime(2024, 3, 3, 11, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔵 от")


def test_image_only_email_has_no_attachments():
    processor = _processor()
    msg = InboundMessage(
        subject="Фотографии",
        sender="studio@example.com",
        body="Смотрите снимки во вложении.",
        attachments=[Attachment(filename="photo.jpg", content=b"", content_type="image/jpeg", text="")],
        received_at=datetime(2024, 4, 4, 12, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    lines = result.split("\n")
    assert len(lines) >= 2


def test_output_has_two_mandatory_lines():
    processor = _processor()
    msg = InboundMessage(
        subject="Напоминание",
        sender="team@example.com",
        body="Проверить статус задач и ответить клиенту.",
        attachments=[Attachment(filename="report.pdf", content=b"", content_type="application/pdf", text="Отчет по задачам")],
        received_at=datetime(2024, 5, 5, 13, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    mandatory = [line for line in result.split("\n") if line.strip()][0:3]
    assert len(mandatory) >= 2
    assert mandatory[0].startswith(("🔴", "🟡", "🔵"))
    assert mandatory[1].split()[0] in MessageProcessor._VERB_ORDER


def test_no_duplicate_attachment_names():
    processor = _processor()
    attachments = [
        Attachment(
            filename="report.pdf",
            content=b"",
            content_type="application/pdf",
            text="""
            Отчет по продажам за месяц включает показатели по регионам,
            динамику и ключевые выводы менеджмента для анализа.
            """,
        ),
        Attachment(
            filename="report.pdf",
            content=b"",
            content_type="application/pdf",
            text="""
            Дублирующий отчет с корректировками, содержит уточненные числа
            и обновленные итоговые данные по продажам.
            """,
        ),
        Attachment(
            filename="contract.docx",
            content=b"",
            content_type="application/msword",
            text="""
            Договор на поставку оборудования с описанием обязательств,
            сроков поставки и условий оплаты по контракту.
            """,
        ),
    ]

    msg = InboundMessage(
        subject="Отчеты и договор",
        sender="ops@example.com",
        body=(
            "Проверьте отчеты во вложении и обновленную версию договора, "
            "нужно подтвердить изменения."
        ),
        attachments=attachments,
        received_at=datetime(2024, 7, 7, 15, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    assert len(attachment_lines) == 3
    filenames = [line.split(" — ")[0] for line in attachment_lines]
    assert filenames.count("report.pdf") == 2
    assert filenames.count("contract.docx") == 1


def test_all_non_image_attachments_are_rendered():
    processor = _processor()
    attachments = [
        Attachment(
            filename="contract.doc",
            content=b"",
            content_type="application/msword",
            text="Общие условия договора на поставку продукции.",
        ),
        Attachment(
            filename="note.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Короткая заметка",
        ),
        Attachment(
            filename="prices.xlsx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            text="Таблица с ценами и кодами товаров",
        ),
        Attachment(
            filename="report.xlsx",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="Отчет по продажам за квартал",
        ),
    ]

    msg = InboundMessage(
        subject="Пакет документов и таблиц",
        sender="ops@example.com",
        body="Высылаем комплект файлов",
        attachments=attachments,
        received_at=datetime(2024, 8, 8, 16, 0),
    )

    result = processor.process("user@example.com", msg)
    assert result is not None

    lines = result.split("\n")
    assert lines[0].strip()
    assert lines[1].strip()

    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)
    assert len(attachment_lines) == 4

    for filename in ["contract.doc", "note.docx", "prices.xlsx", "report.xlsx"]:
        assert any(line.startswith(filename) for line in attachment_lines)


def test_informational_email_remains_blue():
    processor = _processor()
    msg = InboundMessage(
        subject="Hello friend",
        sender="friend@example.com",
        body="Hello friend, happy birthday dear friend!",
        attachments=[],
        received_at=datetime(2024, 6, 6, 14, 0),
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    first_line = result.split("\n")[0]
    assert first_line.startswith("🔵 от")


def test_attachment_lines_drop_prefixes_and_counts():
    processor = _processor()
    attachments = [
        Attachment(
            filename="stats.xlsx",
            content=b"",
            content_type="application/vnd.ms-excel",
            text="Код;Цена;Сумма\n1;10;10\n2;20;40",
        ),
        Attachment(
            filename="notes.docx",
            content=b"",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            text="Проверка с документами",
        ),
    ]

    msg = InboundMessage(
        subject="Таблицы и документы",
        sender="ops@example.com",
        body="",
        attachments=attachments,
    )

    result = processor.process("robot@example.com", msg)
    names = {att.filename for att in attachments}
    attachment_lines = _attachment_lines(result, names)

    assert len(attachment_lines) == 2
    for line in attachment_lines:
        assert "таблица:" not in line
        assert "(≈" not in line


def test_body_placeholder_is_silent_when_empty():
    processor = _processor()
    msg = InboundMessage(
        subject="Файлы",
        sender="sender@example.com",
        body="",
        attachments=[Attachment(filename="info.pdf", content=b"", content_type="application/pdf", text="Вложение")],
    )

    result = processor.process("user@example.com", msg)

    lines = result.split("\n")
    assert len(lines) >= 3
    assert "тело" not in result.lower()
    assert any(line.startswith("info.pdf") for line in _attachment_lines(result, {"info.pdf"}))


def test_normalize_action_subject_deduplicates_tokens():
    processor = _processor()
    action_one = processor._normalize_action_subject(
        "", "Прайс лист", []
    )
    assert action_one == "Проверить цены"

    action_two = processor._normalize_action_subject(
        "", "Проверка с документами", []
    )
    assert action_two == "Проверить письмо"



def test_process_drops_duplicate_subject_line_in_body():
    processor = _processor()
    msg = InboundMessage(
        subject="RE: Счёт за март",
        sender="billing@example.com",
        body="",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)

    lines = result.split("\n")
    assert lines[0].startswith(("🔴", "🟡", "🔵"))
    assert all("<b>" not in line for line in lines[1:])
    assert "Оплатить счёт" in result


def test_duplicate_subject_helper_keeps_non_matching_next_line():
    lines = _maybe_drop_duplicate_subject_line("RE: Счёт", ["Проверить таблицу"])

    assert lines == ["Проверить таблицу"]

def test_action_line_prefers_mail_type_over_excel_attachment():
    processor = _processor()
    msg = InboundMessage(
        subject="Акт сверки за январь",
        sender="finance@example.com",
        body="Во вложении акт сверки и таблица.",
        mail_type="ACT_RECONCILIATION",
        attachments=[
            Attachment(filename="reconciliation.xls", content=b"", content_type="application/vnd.ms-excel", text="")
        ],
    )

    result = processor.process("robot@example.com", msg)
    assert "Проверить акт" in result
    assert "Проверить таблицу" not in result


def test_action_line_invoice_mail_type_overrides_excel_attachment():
    processor = _processor()
    msg = InboundMessage(
        subject="Счет за услуги",
        sender="billing@example.com",
        body="Просим оплатить в срок.",
        mail_type="INVOICE",
        attachments=[
            Attachment(filename="invoice.xls", content=b"", content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", text="")
        ],
    )

    result = processor.process("robot@example.com", msg)
    assert "Оплатить счёт" in result
    assert "Проверить таблицу" not in result


def test_action_line_falls_back_to_subject_price_keywords():
    processor = _processor()
    msg = InboundMessage(
        subject="Обновленные цены на продукцию",
        sender="sales@example.com",
        body="Прайс во вложении.",
        mail_type="",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)
    assert "Проверить цены" in result


def test_action_line_falls_back_to_excel_attachment_when_mail_type_unknown():
    processor = _processor()
    msg = InboundMessage(
        subject="Файл по итогам",
        sender="ops@example.com",
        body="",
        mail_type="",
        attachments=[
            Attachment(filename="totals.xls", content=b"", content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", text="")
        ],
    )

    result = processor.process("robot@example.com", msg)
    assert "Проверить таблицу" in result


def test_action_line_generic_fallback_when_no_signals():
    processor = _processor()
    msg = InboundMessage(
        subject="Информационное письмо",
        sender="info@example.com",
        body="Добрый день. Информация к сведению.",
        mail_type="",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)
    assert "Проверить письмо" in result


def test_heuristic_attachment_summary_uses_extracted_text() -> None:
    summaries = _build_heuristic_attachment_summaries(
        [
            {
                "filename": "invoice.xlsx",
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "text": "Счет №1234 Итого: 87 500 руб. Оплатить до 15.04.2026",
            }
        ]
    )

    assert summaries[0]["filename"] == "invoice.xlsx"
    assert summaries[0]["summary"]
    assert "87 500" in summaries[0]["summary"]


def test_priority_signal_includes_attachment_content_and_facts() -> None:
    signal = _build_priority_signal_text(
        "Письмо без явных маркеров",
        [
            {
                "filename": "invoice.xlsx",
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "text": "Итого: 87 500 руб. Оплатить до 15.04.2026",
            }
        ],
    )

    assert "invoice.xlsx" in signal
    assert "87 500" in signal
    assert "оплатить" in signal.lower()


def test_action_selection_uses_body_and_attachment_text() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Уточнение",
        sender="billing@example.com",
        body="Прошу оплатить счёт в срок",
        attachments=[
            Attachment(
                filename="invoice.xlsx",
                content=b"",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                text="Итого 87 500 руб.",
            )
        ],
    )

    result = processor.process("robot@example.com", msg)
    assert "Оплатить счёт" in result


def test_invoice_body_only_gives_payment_action_and_meaningful_summary() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Уточнение",
        sender="billing@example.com",
        body="Прошу оплатить счёт №445 до 15.04.2026. Итого 87 500 руб.",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)
    assert "Оплатить счёт" in result
    assert "87 500" in result


def test_invoice_attachment_only_influences_action_and_priority() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Документы",
        sender="billing@example.com",
        body="",
        attachments=[
            Attachment(
                filename="table.xlsx",
                content=b"",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                text="Итого: 87 500 руб. Оплатить до 15.04.2026",
            )
        ],
    )

    result = processor.process("robot@example.com", msg)
    assert result.split("\n")[0].startswith(("🟡 от", "🔴 от"))
    assert "Оплатить счёт" in result


def test_contract_signature_case_avoids_generic_output() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Подписание договора",
        sender="legal@example.com",
        body="Нужно подписать договор и вернуть экземпляр.",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)
    assert "Проверить договор" in result
    assert "Проверить письмо" not in result


def test_action_selection_avoids_payment_for_incident_signals() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Сервис недоступен",
        sender="ops@example.com",
        body="Платежный шлюз offline, авария в проде",
        attachments=[],
    )

    result = processor.process("robot@example.com", msg)
    assert "Оплатить счёт" not in result
    assert "Проверить" in result


def test_sender_memory_uplift_bounded_for_repeated_escalations(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_processor,
        "_sender_memory_bias_for_priority",
        lambda **_kwargs: (12, 4),
    )

    result = _apply_sender_memory_to_priority(
        priority="🔵",
        sender_email="vip.sender@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=1,
    )

    assert result == "🟡"


def test_sender_memory_dampening_bounded_for_repeated_demotions(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_processor,
        "_sender_memory_bias_for_priority",
        lambda **_kwargs: (-12, 5),
    )

    result = _apply_sender_memory_to_priority(
        priority="🟡",
        sender_email="noisy.sender@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=2,
    )

    assert result == "🔵"


def test_sender_memory_insufficient_history_no_change(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_processor,
        "_sender_memory_bias_for_priority",
        lambda **_kwargs: (0, 2),
    )

    result = _apply_sender_memory_to_priority(
        priority="🟡",
        sender_email="sender@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=3,
    )

    assert result == "🟡"


def test_sender_memory_dampening_does_not_suppress_high_signal_mail(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_processor,
        "_sender_memory_bias_for_priority",
        lambda **_kwargs: (-12, 6),
    )

    result = _apply_sender_memory_to_priority(
        priority="🟡",
        sender_email="finance@example.com",
        mail_type="INVOICE_FINAL",
        priority_v2_result=PriorityResultV2(
            priority="🟡",
            score=55,
            breakdown=(),
            reason_codes=("PRIO_INVOICE_SUBJECT",),
        ),
        email_id=4,
    )

    assert result == "🟡"


def test_sender_memory_scoped_per_sender(monkeypatch) -> None:
    def _fake_bias(*, sender_email: str) -> tuple[int, int]:
        if sender_email == "fav@example.com":
            return (12, 5)
        return (0, 5)

    monkeypatch.setattr(pipeline_processor, "_sender_memory_bias_for_priority", _fake_bias)

    favored = _apply_sender_memory_to_priority(
        priority="🔵",
        sender_email="fav@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=5,
    )
    other = _apply_sender_memory_to_priority(
        priority="🔵",
        sender_email="other@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=6,
    )

    assert favored == "🟡"
    assert other == "🔵"


def test_sender_memory_empty_sender_or_query_failure_keeps_priority(monkeypatch) -> None:
    empty_sender = _apply_sender_memory_to_priority(
        priority="🔵",
        sender_email="",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=7,
    )
    assert empty_sender == "🔵"

    def _raise_connect(_path: object):
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(pipeline_processor.sqlite3, "connect", _raise_connect)
    bias, sample_count = pipeline_processor._sender_memory_bias_for_priority(
        sender_email="sender@example.com"
    )
    assert bias == 0
    assert sample_count == 0

    no_data = _apply_sender_memory_to_priority(
        priority="🟡",
        sender_email="sender@example.com",
        mail_type="UNKNOWN",
        priority_v2_result=None,
        email_id=8,
    )

    assert no_data == "🟡"


def test_decision_layer_keeps_priority_action_consistent() -> None:
    facts = _collect_message_facts(
        subject="Сервис недоступен",
        body_text="offline incident, security alert",
        attachments=[],
        mail_type="INCIDENT",
    )

    decision = _build_message_decision(
        priority="🔴",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
    )

    assert decision.priority == "🔴"
    assert "оплат" not in decision.action.lower()


def test_invoice_decision_produces_payment_action() -> None:
    facts = _collect_message_facts(
        subject="Счет №445",
        body_text="Итого 87 500 руб. Оплатить до 15.04.2026",
        attachments=[],
        mail_type="",
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Проверить",
        summary="",
        message_facts=facts,
    )

    assert decision.doc_kind == "invoice"
    assert decision.action == "Оплатить"


def test_incident_decision_never_returns_payment_action() -> None:
    facts = _collect_message_facts(
        subject="Security alert",
        body_text="Подозрительный вход и offline сервисов",
        attachments=[],
        mail_type="SECURITY_ALERT",
    )

    decision = _build_message_decision(
        priority="🔴",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
    )

    assert decision.doc_kind == "incident"
    assert decision.action == "Зафиксировать"


def test_confidence_high_for_invoice_facts() -> None:
    attachments: list[dict[str, object]] = []
    facts = _collect_message_facts(
        subject="Счет №445",
        body_text="Итого 87 500 руб. Оплатить до 15.04.2026",
        attachments=attachments,
        mail_type="",
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Проверить",
        summary="",
        message_facts=facts,
        subject="Счет №445",
        body_text="Итого 87 500 руб. Оплатить до 15.04.2026",
        attachments=attachments,
    )

    assert decision.confidence >= 0.75
    assert decision.action == "Оплатить"


def test_confidence_low_for_filename_only_invoice() -> None:
    attachments = [{"filename": "invoice_april.pdf", "text": ""}]
    facts = _collect_message_facts(
        subject="Документы",
        body_text="См. вложение",
        attachments=attachments,
        mail_type="INVOICE",
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Проверить",
        summary="",
        message_facts=facts,
        subject="Документы",
        body_text="См. вложение",
        attachments=attachments,
    )

    assert decision.doc_kind == "invoice"
    assert decision.confidence < 0.45


def test_low_confidence_softens_action() -> None:
    attachments = [{"filename": "invoice_april.pdf", "text": ""}]
    facts = _collect_message_facts(
        subject="Документы",
        body_text="См. вложение",
        attachments=attachments,
        mail_type="INVOICE",
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
        subject="Документы",
        body_text="См. вложение",
        attachments=attachments,
    )

    assert decision.confidence < 0.45
    assert decision.action == "Проверить"


def test_incident_confidence_high() -> None:
    attachments: list[dict[str, object]] = []
    facts = _collect_message_facts(
        subject="Security alert",
        body_text="Подозрительный вход и offline сервисов",
        attachments=attachments,
        mail_type="INCIDENT",
    )

    decision = _build_message_decision(
        priority="🔴",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
        subject="Security alert",
        body_text="Подозрительный вход и offline сервисов",
        attachments=attachments,
    )

    assert decision.confidence >= 0.7
    assert decision.action == "Зафиксировать"


def test_summary_uses_decision_facts() -> None:
    facts = _collect_message_facts(
        subject="Счет",
        body_text="Итого 87 500 руб. Оплатить до 15.04.2026",
        attachments=[],
        mail_type="",
    )

    summary = _build_heuristic_summary(
        subject="Счет",
        body_text="Коротко",
        attachments=[],
        message_facts=facts,
    )

    assert "87 500" in summary
    assert "15.04.2026" in summary




def test_amount_scoring_prefers_total_keyword() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}

    scored = _score_message_facts(
        facts,
        evidence_text="91000 строка 87500 итого 87500 руб",
        attachment_text="",
    )

    assert scored["amount"] == "87500 руб"


def test_amount_scoring_prefers_currency_context() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}

    scored = _score_message_facts(
        facts,
        evidence_text="сумма 91000 и к оплате 87500 usd",
        attachment_text="",
    )

    assert scored["amount"] == "87500"


def test_amount_scoring_ignores_table_row_number() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}

    scored = _score_message_facts(
        facts,
        evidence_text="строка 87500 итоговый платеж 95000 руб",
        attachment_text="",
    )

    assert scored["amount"] == "95000 руб"


def test_amount_scoring_attachment_context() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}

    scored = _score_message_facts(
        facts,
        evidence_text="в письме 91000, во вложении amount due 87500 usd",
        attachment_text="amount due 87500 usd",
    )

    assert scored["amount"] == "87500"

def test_validate_amount_filters_table_numbers() -> None:
    facts = {
        "amount": "150",
        "due_date": "",
        "doc_number": "",
        "doc_kind": "invoice",
    }

    validated = _validate_message_facts(facts, evidence_text="таблица: 150 200 250")

    assert validated["amount"] == ""


def test_validate_due_date_range() -> None:
    facts = {
        "amount": "87 500",
        "due_date": "01.01.2099",
        "doc_number": "INV-44",
    }

    validated = _validate_message_facts(facts, evidence_text="к оплате 87 500 руб")

    assert validated["due_date"] == ""


def test_validate_doc_number_reasonable_length() -> None:
    facts = {
        "amount": "87 500",
        "due_date": "15.04.2026",
        "doc_number": "AB",
    }

    validated = _validate_message_facts(facts, evidence_text="счет №AB сумма 87 500 руб")

    assert validated["doc_number"] == ""


def test_valid_invoice_facts_preserved() -> None:
    facts = _collect_message_facts(
        subject="Счет №445",
        body_text="87 500 руб. Оплатить до 15.04.2026",
        attachments=[],
        mail_type="INVOICE",
    )

    validated = _validate_message_facts(
        facts,
        evidence_text="Счет №445 87 500 руб. Оплатить до 15.04.2026",
    )

    assert validated["amount"]
    assert validated["due_date"] == "15.04.2026"
    assert validated["doc_number"] == "445"


def test_reply_payment_confirmation_not_pay_action() -> None:
    facts = _collect_message_facts(
        subject="RE: счет",
        body_text="оплатили вчера",
        attachments=[],
        mail_type="INVOICE",
    )
    facts = _validate_message_facts(facts, evidence_text="RE: счет оплатили вчера")
    context = _detect_conversation_context(subject="RE: счет", body_text="оплатили вчера", message_facts=facts)

    decision = _build_message_decision(
        priority="🟡",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
        subject="RE: счет",
        body_text="оплатили вчера",
        attachments=[],
        context=context,
    )

    assert context == "CONFIRMATION"
    assert decision.context == "CONFIRMATION"
    assert "оплат" not in decision.action.lower()


def test_forward_contract_discussion_not_final_action() -> None:
    facts = _collect_message_facts(
        subject="FW: договор",
        body_text="вот правка, комментарий по пункту 4",
        attachments=[],
        mail_type="CONTRACT",
    )
    facts = _validate_message_facts(facts, evidence_text="FW: договор вот правка")
    context = _detect_conversation_context(
        subject="FW: договор",
        body_text="вот правка, комментарий по пункту 4",
        message_facts=facts,
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Согласовать договор",
        summary="",
        message_facts=facts,
        subject="FW: договор",
        body_text="вот правка, комментарий по пункту 4",
        attachments=[],
        context=context,
    )

    assert context == "DISCUSSION"
    assert decision.context == "DISCUSSION"
    assert "соглас" not in decision.action.lower()


def test_new_invoice_keeps_pay_action() -> None:
    facts = _collect_message_facts(
        subject="новый счет",
        body_text="к оплате 87 500 руб до 15.04.2026",
        attachments=[],
        mail_type="INVOICE",
    )
    facts = _validate_message_facts(facts, evidence_text="новый счет к оплате 87 500 руб")
    context = _detect_conversation_context(
        subject="новый счет",
        body_text="к оплате 87 500 руб до 15.04.2026",
        message_facts=facts,
    )

    decision = _build_message_decision(
        priority="🟡",
        action_line="Оплатить",
        summary="",
        message_facts=facts,
        subject="новый счет",
        body_text="к оплате 87 500 руб до 15.04.2026",
        attachments=[],
        context=context,
    )

    assert context == "NEW_MESSAGE"
    assert decision.action == "Оплатить"


def test_context_detection_reply() -> None:
    context = _detect_conversation_context(
        subject="RE: Счет №123",
        body_text="Просьба подтвердить",
        message_facts={"invoice_signal": True},
    )

    assert context == "REPLY"


def test_context_detection_forward() -> None:
    context = _detect_conversation_context(
        subject="FW: договор",
        body_text="посмотрите пункт 4",
        message_facts={"contract_signal": True},
    )

    assert context == "FORWARD"

def test_document_identity_same_invoice_detected() -> None:
    facts = {"doc_kind": "invoice", "doc_number": "123", "amount": "87 500"}

    first_id = _build_document_identity(
        message_facts=facts,
        sender_email="vendor@example.com",
        subject="Счет №123",
    )
    second_id = _build_document_identity(
        message_facts=facts,
        sender_email="vendor@example.com",
        subject="Счет №123",
    )

    assert first_id == second_id


def test_document_identity_forward_detected() -> None:
    facts = {"doc_kind": "invoice", "doc_number": "123", "amount": "87 500"}

    base_id = _build_document_identity(
        message_facts=facts,
        sender_email="vendor@example.com",
        subject="Счет №123",
    )
    forward_id = _build_document_identity(
        message_facts=facts,
        sender_email="vendor@example.com",
        subject="FW: Счет №123",
    )

    assert forward_id == base_id


def test_document_identity_new_invoice_not_duplicate() -> None:
    first_id = _build_document_identity(
        message_facts={"doc_kind": "invoice", "doc_number": "123", "amount": "87 500"},
        sender_email="vendor@example.com",
        subject="Счет №123",
    )
    second_id = _build_document_identity(
        message_facts={"doc_kind": "invoice", "doc_number": "124", "amount": "87 500"},
        sender_email="vendor@example.com",
        subject="Счет №124",
    )

    assert first_id != second_id


def test_duplicate_softens_action() -> None:
    assert _soften_duplicate_action("Оплатить") == "Зафиксировать"
    assert _soften_duplicate_action("Проверить") == "Проверить"


def test_relationship_profile_counts(tmp_path) -> None:
    db_path = tmp_path / "relationship-counts.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Счет №1", "invoice to pay", now.isoformat(), now.isoformat()),
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Договор поставки", "contract draft", now.isoformat(), now.isoformat()),
        )
        conn.execute(
            """
            INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "commitment_overdue",
                now.timestamp(),
                now.isoformat(),
                "user@example.com",
                "vendor",
                1,
                '{"sender_email": "vendor@example.com"}',
                '{"sender_email": "vendor@example.com"}',
                1,
                "rel-counts-overdue",
            ),
        )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    profile = _build_sender_relationship_profile(
        analytics=analytics,
        account_email="user@example.com",
        sender_email="vendor@example.com",
    )
    assert profile is not None
    assert profile["emails_count"] == 2
    assert profile["invoice_count"] == 1
    assert profile["contract_count"] == 1
    assert profile["overdue_count"] == 1


def test_relationship_last_contact_days(tmp_path) -> None:
    db_path = tmp_path / "relationship-last-contact.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=5, hours=2)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Ping", "status update", old.isoformat(), old.isoformat()),
        )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    profile = analytics.sender_relationship_profile(
        account_email="user@example.com",
        sender_email="vendor@example.com",
        now=now,
    )
    assert profile is not None
    assert profile["last_contact_days"] == 5


def test_relationship_trust_score(tmp_path) -> None:
    db_path = tmp_path / "relationship-trust.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Счет", "invoice", now.isoformat(), now.isoformat()),
        )
        events = [
            ("invoice_paid", '{"sender_email": "vendor@example.com"}', "rel-trust-paid"),
            ("fast_reply", '{"sender_email": "vendor@example.com"}', "rel-trust-fast"),
            ("dispute_opened", '{"sender_email": "vendor@example.com"}', "rel-trust-dispute"),
            ("commitment_overdue", '{"sender_email": "vendor@example.com"}', "rel-trust-overdue"),
        ]
        for event_type, payload, fingerprint in events:
            conn.execute(
                """
                INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (event_type, now.timestamp(), now.isoformat(), "user@example.com", "vendor", 1, payload, payload, 1, fingerprint),
            )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    profile = analytics.sender_relationship_profile(
        account_email="user@example.com",
        sender_email="vendor@example.com",
        now=now,
    )
    assert profile is not None
    assert profile["trust_score"] == 0


def test_relationship_invoice_tracking(tmp_path) -> None:
    db_path = tmp_path / "relationship-invoice.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Invoice #1", "please pay", now.isoformat(), now.isoformat()),
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("user@example.com", "vendor@example.com", "Invoice #2", "invoice attached", now.isoformat(), now.isoformat()),
        )
        conn.commit()

    analytics = KnowledgeAnalytics(db_path)
    profile = analytics.sender_relationship_profile(
        account_email="user@example.com",
        sender_email="vendor@example.com",
        now=now,
    )
    assert profile is not None
    assert profile["invoice_count"] == 2


def test_payroll_never_classified_as_invoice_action() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Расчетный листок за февраль",
        sender="hr@example.com",
        body="Начислено 120 000 руб, удержано 15 000 руб, к выплате 105 000 руб. Дата документа 05.03.2026",
        attachments=[],
        received_at=datetime(2026, 3, 5, 10, 0),
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "Оплатить счёт" not in result


def test_payroll_does_not_emit_invoice_amount_due() -> None:
    processor = _processor()
    msg = InboundMessage(
        subject="Расчётный листок",
        sender="hr@example.com",
        body="Начислено 4 848 150. Удержано 120 000. Дата документа 05.03.2026",
        attachments=[],
        received_at=datetime(2026, 3, 5, 10, 0),
    )

    result = processor.process("user@example.com", msg)

    assert result is not None
    assert "₽ · до" not in result
    assert "до 05.03.2026" not in result


def test_invoice_amount_prefers_total_to_pay_keywords() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}

    scored = _score_message_facts(
        facts,
        evidence_text="Начислено 4 848 150 руб. Итого к оплате 87 500 руб. Оплатить до 15.04.2026",
    )

    assert scored["amount"].startswith("87 500 руб")


def test_invoice_body_amount_detection() -> None:
    body = "\u041d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e 120 000 \u0440\u0443\u0431. \u0423\u0434\u0435\u0440\u0436\u0430\u043d\u043e 32 500 \u0440\u0443\u0431. \u0418\u0442\u043e\u0433\u043e \u043a \u043e\u043f\u043b\u0430\u0442\u0435 87 500 \u0440\u0443\u0431."
    facts = _collect_message_facts(
        subject="\u0421\u0447\u0435\u0442 \u2116455",
        body_text=body,
        attachments=[],
        mail_type="INVOICE",
    )
    facts = _validate_message_facts(facts, evidence_text=body)
    facts = _score_message_facts(facts, evidence_text=body, attachment_text="")

    assert facts["invoice_signal"] is True
    assert facts["amount"].startswith("87 500")

def test_forwarded_thread_not_used_for_fact_extraction() -> None:
    test_forwarded_thread_not_used()


def test_quoted_reply_not_used_for_fact_extraction() -> None:
    body = (
        "\u041d\u043e\u0432\u044b\u0439 \u0441\u0442\u0430\u0442\u0443\u0441 \u043f\u043e \u0441\u0447\u0435\u0442\u0443.\n"
        "> \u0418\u0442\u043e\u0433\u043e \u043a \u043e\u043f\u043b\u0430\u0442\u0435 999 999 \u0440\u0443\u0431.\n"
        "> From: old@example.com"
    )
    facts = _collect_message_facts(
        subject="\u0421\u0447\u0435\u0442 \u2116455",
        body_text=body,
        attachments=[],
        mail_type="INVOICE",
    )
    assert facts["amount"] == ""


def test_main_body_numbers_still_detected() -> None:
    test_main_body_numbers_detected()


def test_invoice_attachment_amount_detection() -> None:
    attachment_text = "Invoice #7 total payable 4200 USD amount due 4200 USD"
    attachments = [
        {
            "filename": "invoice_07.pdf",
            "content_type": "application/pdf",
            "text": attachment_text,
        }
    ]
    evidence_text = f"\u0421\u043c. \u0432\u043b\u043e\u0436\u0435\u043d\u0438\u0435 {attachment_text}"

    facts = _collect_message_facts(
        subject="\u0414\u043e\u043a\u0443\u043c\u0435\u043d\u0442\u044b",
        body_text="\u0421\u043c. \u0432\u043b\u043e\u0436\u0435\u043d\u0438\u0435",
        attachments=attachments,
        mail_type="",
    )
    facts = _validate_message_facts(facts, evidence_text=evidence_text)
    facts = _score_message_facts(facts, evidence_text=evidence_text, attachment_text=attachment_text)

    assert facts["invoice_signal"] is True
    assert facts["amount"].startswith("4200")

def test_pdf_table_total_preferred_over_global_number_match() -> None:
    attachment_text = (
        "\u041f\u043e\u0437\u0438\u0446\u0438\u044f\t\u0426\u0435\u043d\u0430\n"
        "\u0423\u0441\u043b\u0443\u0433\u0430\t12 500 \u0440\u0443\u0431\n"
        "\u0418\u0442\u043e\u0433\u043e \u043a \u043e\u043f\u043b\u0430\u0442\u0435\t87 500 \u0440\u0443\u0431"
    )
    attachments = [
        {
            "filename": "invoice_table.pdf",
            "content_type": "application/pdf",
            "text": attachment_text,
        }
    ]
    evidence_text = "\u041d\u043e\u043c\u0435\u0440 \u0434\u043e\u0433\u043e\u0432\u043e\u0440\u0430 999 999 \u0440\u0443\u0431. " + attachment_text

    facts = _collect_message_facts(
        subject="\u0421\u0447\u0435\u0442 \u043d\u0430 \u043e\u043f\u043b\u0430\u0442\u0443",
        body_text="\u0421\u043c. \u0432\u043b\u043e\u0436\u0435\u043d\u0438\u0435",
        attachments=attachments,
        mail_type="INVOICE",
    )
    facts = _validate_message_facts(facts, evidence_text=evidence_text)
    facts = _score_message_facts(facts, evidence_text=evidence_text, attachment_text=attachment_text)

    assert facts["amount"].startswith("87 500")


def test_invoice_attachment_amount_extracted_from_table_context() -> None:
    attachment_text = (
        "item\tqty\tprice\n"
        "service\t1\t1200 USD\n"
        "total payable\t4200 USD\n"
    )
    attachments = [
        {
            "filename": "invoice_table.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": attachment_text,
        }
    ]
    evidence_text = "invoice attached " + attachment_text

    facts = _collect_message_facts(
        subject="Invoice",
        body_text="See attachment",
        attachments=attachments,
        mail_type="",
    )
    facts = _validate_message_facts(facts, evidence_text=evidence_text)
    facts = _score_message_facts(facts, evidence_text=evidence_text, attachment_text=attachment_text)

    assert facts["amount"].startswith("4200")


def test_attachment_without_currency_does_not_become_total() -> None:
    attachment_text = (
        "item\tqty\tprice\n"
        "service\t1\t12500\n"
        "total payable\t87500\n"
    )
    attachments = [
        {
            "filename": "invoice_table.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": attachment_text,
        }
    ]
    evidence_text = "invoice attached " + attachment_text

    facts = _collect_message_facts(
        subject="Invoice",
        body_text="See attachment",
        attachments=attachments,
        mail_type="INVOICE",
    )
    facts = _validate_message_facts(facts, evidence_text=evidence_text)
    facts = _score_message_facts(facts, evidence_text=evidence_text, attachment_text=attachment_text)

    assert facts["amount"] == ""


def test_payroll_attachment_never_invoice_action() -> None:
    attachment_text = (
        "\u041d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e\t120 000 \u0440\u0443\u0431\n"
        "\u0423\u0434\u0435\u0440\u0436\u0430\u043d\u043e\t15 000 \u0440\u0443\u0431\n"
        "\u041a \u0432\u044b\u043f\u043b\u0430\u0442\u0435\t105 000 \u0440\u0443\u0431"
    )
    attachments = [
        {
            "filename": "\u0440\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439_\u043b\u0438\u0441\u0442\u043e\u043a.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text": attachment_text,
        }
    ]
    evidence_text = "\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a " + attachment_text

    facts = _collect_message_facts(
        subject="\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
        body_text="\u0421\u043c. \u0432\u043b\u043e\u0436\u0435\u043d\u0438\u0435",
        attachments=attachments,
        mail_type="",
    )
    facts = _validate_message_facts(facts, evidence_text=evidence_text)
    facts = _score_message_facts(facts, evidence_text=evidence_text, attachment_text=attachment_text)
    facts = _consistency_check_message_facts(facts, evidence_text=evidence_text)

    decision = _build_message_decision(
        priority="\U0001f7e1",
        action_line="\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c",
        summary="",
        message_facts=facts,
        subject="\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
        body_text=evidence_text,
        attachments=attachments,
        context="NEW_MESSAGE",
    )

    assert facts["doc_kind"] == "payroll"
    assert "\u043e\u043f\u043b\u0430\u0442" not in decision.action.lower()


def test_telegram_uses_interpretation_not_raw_facts() -> None:
    interpretation = MessageInterpretation(
        email_id="42",
        sender_email="vendor@example.com",
        doc_kind="invoice",
        amount=87500.0,
        due_date="15.04.2026",
        action="Pay now",
        priority="\U0001f534",
        confidence=0.91,
        context="NEW_MESSAGE",
        document_id="invoice_42_vendor",
    )

    rendered = _build_telegram_text(
        priority="\U0001f534",
        from_email="vendor@example.com",
        subject="\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
        action_line="Review manually",
        mail_type="PAYROLL",
        body_summary="\u041d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e 120 000 \u0440\u0443\u0431",
        body_text="\u041d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e 120 000 \u0440\u0443\u0431, \u0443\u0434\u0435\u0440\u0436\u0430\u043d\u043e 15 000 \u0440\u0443\u0431",
        attachments=[],
        interpretation=interpretation,
    )

    assert "Pay now" in rendered
    assert "87 500" in rendered


def test_invoice_math_consistency() -> None:
    base_facts = {
        "amount": "120 USD",
        "due_date": "15.03.2026",
        "doc_number": "INV-1",
        "doc_kind": "invoice",
        "invoice_signal": True,
        "payroll_signal": False,
        "contract_signal": False,
        "incident_signal": False,
        "amount_context_missing": False,
    }
    consistent_evidence = "Subtotal 100 USD Tax 20 USD Total 120 USD Invoice date: 10.03.2026"
    inconsistent_evidence = "Subtotal 100 USD Tax 20 USD Total 170 USD Invoice date: 10.03.2026"

    consistent = _consistency_check_message_facts(base_facts, evidence_text=consistent_evidence)
    inconsistent = _consistency_check_message_facts(base_facts, evidence_text=inconsistent_evidence)

    consistent_decision = _build_message_decision(
        priority="\U0001f7e1",
        action_line="Pay invoice",
        summary="",
        message_facts=consistent,
        subject="Invoice",
        body_text=consistent_evidence,
        attachments=[],
        context="NEW_MESSAGE",
    )
    inconsistent_decision = _build_message_decision(
        priority="\U0001f7e1",
        action_line="Pay invoice",
        summary="",
        message_facts=inconsistent,
        subject="Invoice",
        body_text=inconsistent_evidence,
        attachments=[],
        context="NEW_MESSAGE",
    )

    assert "subtotal_tax_total_mismatch" in inconsistent["consistency_issues"]
    assert inconsistent["consistency_penalty"] > consistent["consistency_penalty"]
    assert inconsistent_decision.confidence < consistent_decision.confidence


def test_currency_context_required() -> None:
    with_currency = {
        "amount": "87 500 USD",
        "due_date": "15.03.2026",
        "doc_number": "INV-2",
        "doc_kind": "invoice",
        "invoice_signal": True,
        "payroll_signal": False,
        "contract_signal": False,
        "incident_signal": False,
        "amount_context_missing": False,
    }
    without_currency = dict(with_currency)
    without_currency["amount"] = "87 500"
    without_currency["amount_context_missing"] = True

    with_currency_checked = _consistency_check_message_facts(
        with_currency,
        evidence_text="Invoice total 87 500 USD amount due 87 500 USD",
    )
    without_currency_checked = _consistency_check_message_facts(
        without_currency,
        evidence_text="Invoice total 87 500 amount due 87 500",
    )

    assert "amount_without_currency" not in with_currency_checked["consistency_issues"]
    assert "amount_without_currency" in without_currency_checked["consistency_issues"]
    assert without_currency_checked["consistency_penalty"] > with_currency_checked["consistency_penalty"]


def test_payroll_never_invoice() -> None:
    body = "\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a: \u043d\u0430\u0447\u0438\u0441\u043b\u0435\u043d\u043e 120 000 \u0440\u0443\u0431, \u0443\u0434\u0435\u0440\u0436\u0430\u043d\u043e 15 000 \u0440\u0443\u0431, \u043a \u0432\u044b\u043f\u043b\u0430\u0442\u0435 105 000 \u0440\u0443\u0431, \u0438\u0442\u043e\u0433\u043e \u043a \u043e\u043f\u043b\u0430\u0442\u0435 105 000 \u0440\u0443\u0431"
    facts = _collect_message_facts(
        subject="\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a \u0437\u0430 \u043c\u0430\u0440\u0442",
        body_text=body,
        attachments=[],
        mail_type="",
    )
    facts["invoice_signal"] = True
    facts["doc_kind"] = "invoice"
    facts = _consistency_check_message_facts(facts, evidence_text=body)

    decision = _build_message_decision(
        priority="\U0001f7e1",
        action_line="\u041e\u043f\u043b\u0430\u0442\u0438\u0442\u044c",
        summary="",
        message_facts=facts,
        subject="\u0420\u0430\u0441\u0447\u0435\u0442\u043d\u044b\u0439 \u043b\u0438\u0441\u0442\u043e\u043a",
        body_text=body,
        attachments=[],
        context="NEW_MESSAGE",
    )

    assert facts["payroll_signal"] is True
    assert facts["invoice_signal"] is False
    assert facts["doc_kind"] == "payroll"
    assert "\u043e\u043f\u043b\u0430\u0442" not in decision.action.lower()


def test_invalid_due_date_ignored() -> None:
    facts = {
        "amount": "120 USD",
        "due_date": "05.03.2026",
        "doc_number": "INV-3",
        "doc_kind": "invoice",
        "invoice_signal": True,
        "payroll_signal": False,
        "contract_signal": False,
        "incident_signal": False,
        "amount_context_missing": False,
    }

    checked = _consistency_check_message_facts(
        facts,
        evidence_text="Invoice date: 10.03.2026 Due date: 05.03.2026 Total 120 USD",
    )

    assert checked["due_date"] == ""
    assert "due_date_not_after_invoice_date" in checked["consistency_issues"]
    assert checked["consistency_penalty"] > 0

def test_table_numbers_not_selected() -> None:
    facts = {"amount": "", "due_date": "", "doc_number": "", "doc_kind": "invoice"}
    evidence = "\u0442\u0430\u0431\u043b\u0438\u0446\u0430\n100\n200\n3000\n4000"

    scored = _score_message_facts(
        facts,
        evidence_text=evidence,
        attachment_text=evidence,
    )

    assert scored["amount"] == ""


def test_forwarded_thread_not_used() -> None:
    body = (
        "Новый статус по письму. Проверьте детали.\n"
        "Forwarded message\n"
        "From: old@example.com\n"
        "Итого к оплате 999 999 руб."
    )

    facts = _collect_message_facts(
        subject="Счет №455",
        body_text=body,
        attachments=[],
        mail_type="INVOICE",
    )

    assert facts["amount"] == ""


def test_signature_numbers_not_used() -> None:
    body = (
        "Вопрос по документу. Дайте комментарий.\n"
        "Best regards\n"
        "Finance Team\n"
        "Итого к оплате 777 000 руб."
    )

    facts = _collect_message_facts(
        subject="Документ",
        body_text=body,
        attachments=[],
        mail_type="UNKNOWN",
    )

    assert facts["amount"] == ""


def test_main_body_numbers_detected() -> None:
    body = (
        "Новый счет: итого к оплате 87 500 руб.\n"
        "Best regards\n"
        "Finance Team\n"
        "Итого к оплате 777 000 руб.\n"
        "Forwarded message\n"
        "From: old@example.com\n"
        "Итого к оплате 999 999 руб."
    )

    facts = _collect_message_facts(
        subject="Счет №455",
        body_text=body,
        attachments=[],
        mail_type="INVOICE",
    )

    assert facts["amount"].startswith("87 500")
