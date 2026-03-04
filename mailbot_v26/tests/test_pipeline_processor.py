from datetime import datetime
import sqlite3
import re
from types import SimpleNamespace

from mailbot_v26.pipeline import processor as pipeline_processor
from mailbot_v26.pipeline.processor import (
    Attachment,
    InboundMessage,
    MessageProcessor,
    PriorityResultV2,
    _apply_sender_memory_to_priority,
    _build_heuristic_attachment_summaries,
    _build_priority_signal_text,
    _build_message_decision,
    _build_heuristic_summary,
    _collect_message_facts,
    _maybe_drop_duplicate_subject_line,
)


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
