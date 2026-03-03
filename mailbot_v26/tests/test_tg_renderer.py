from __future__ import annotations

from mailbot_v26.pipeline import tg_renderer


def test_tg_render_standard() -> None:
    attachments = [
        {"filename": "report.pdf", "text": "summary"},
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Проверить письмо",
        attachments=attachments,
    )

    assert "📎 1 вложение: report.pdf" in rendered
    assert "<b><i>Проверить письмо</i></b>" in rendered


def test_tg_no_brackets() -> None:
    attachments = [
        {"filename": "doc1.doc", "text": "snippet"},
    ]

    rendered = tg_renderer.format_attachments_block(attachments)

    assert "[doc1.doc]" not in rendered
    assert "doc1.doc — <i>snippet</i>" in rendered


def test_binary_suppression() -> None:
    attachments = [
        {"filename": "dump.bin", "text": "data=b'\\x00\\x01\\x02'"},
        {"filename": "raw.bin", "text": "b'\\x00\\x01\\x02'"},
    ]

    rendered = tg_renderer.format_attachments_block(attachments)

    assert "dump.bin" in rendered
    assert "raw.bin" in rendered
    assert "dump.bin — <i>" not in rendered
    assert "raw.bin — <i>" not in rendered
    assert "data=b'\\x00" not in rendered


def test_tg_render_dedup_summary_equals_action_line() -> None:
    rendered = tg_renderer.render_telegram_message(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Проверить договор",
        summary="Проверить договор",
        attachments=[],
    )

    assert rendered.count("Проверить договор") == 1


def test_tg_render_drops_duplicate_insight() -> None:
    fields = tg_renderer.apply_semantic_gates(
        action_line="Ответить на письмо",
        summary="Риск срыва дедлайна",
        insights=["Риск срыва дедлайна"],
    )

    assert fields.insights == ()


def test_tg_render_dedup_almost_identical_sentences() -> None:
    deduped = tg_renderer.dedup_sentences(
        ["Согласовать оплату счета", "Оплату счета согласовать"]
    )

    assert deduped == ["Согласовать оплату счета"]


def test_tg_render_skips_short_summary() -> None:
    rendered = tg_renderer.render_telegram_message(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Ответить",
        summary="ок",
        attachments=[],
    )

    assert "ок" not in rendered


def test_summary_sentence_dedup() -> None:
    fields = tg_renderer.apply_semantic_gates(
        action_line="",
        summary="Сделай отчёт сегодня. Сделай отчёт сегодня. Дедлайн сегодня.",
    )

    assert fields.summary == "• Сделай отчёт сегодня\n• Дедлайн сегодня"


def test_action_vs_summary_duplicate_removed() -> None:
    fields = tg_renderer.apply_semantic_gates(
        action_line="Позвонить клиенту сегодня",
        summary="Позвонить клиенту сегодня. Позвонить клиенту сегодня.",
    )

    assert fields.summary == ""


def test_insights_commitments_internal_duplicates_removed() -> None:
    fields = tg_renderer.apply_semantic_gates(
        action_line="",
        summary="",
        insights=["Риск задержки высокий. Риск задержки высокий."],
        commitments=[
            "Обещал отправить договор завтра. Обещал отправить договор завтра."
        ],
    )

    assert fields.insights == ("Риск задержки высокий.",)
    assert fields.commitments == ("Обещал отправить договор завтра.",)


def test_tg_render_drops_duplicate_subject_in_body_first_line() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="  Счёт   на оплату  ",
        action_line="счёт на   оплату",
        attachments=[],
    )

    assert rendered.count("Счёт") == 1


def test_tg_render_keeps_non_duplicate_body_first_line() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Счёт",
        action_line="Оплатить цены",
        attachments=[],
    )

    assert "<b><i>Оплатить цены</i></b>" in rendered


def test_tg_render_drops_subject_duplicate_with_fw_re_prefix() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="FW: Счет",
        action_line="Re: счет",
        attachments=[],
    )

    assert rendered.count("Счет") == 1


def test_tg_render_empty_subject_is_stable() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="",
        action_line="",
        attachments=[],
    )

    assert "<b>(без темы)</b>" in rendered


def test_tg_render_drops_duplicate_subject_with_whitespace_and_fwd_prefix() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="  fWd:   Contract   Update  ",
        action_line=" Re : contract update ",
        attachments=[],
    )

    assert rendered.lower().count("contract") == 1


def test_tg_render_keeps_body_line_when_subjects_differ_after_normalization() -> None:
    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Contract update",
        action_line="Please call tomorrow",
        attachments=[],
    )

    assert "<b>Contract update</b>" in rendered
    assert "<b><i>Please call tomorrow</i></b>" in rendered


def test_attachment_insight_invoice_amount_due_date() -> None:
    attachments = [
        {
            "filename": "invoice.pdf",
            "text": "Итого 58200 руб. Оплатить до 28.02.2026",
        }
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔴",
        from_email="sender@example.com",
        subject="Счет",
        action_line="Оплатить",
        attachments=attachments,
        mail_type="INVOICE",
    )

    assert "📎 Счёт: 58 200 ₽ · до 28.02" in rendered
    assert "invoice.pdf" not in rendered


def test_attachment_insight_act_reconciliation_period() -> None:
    attachments = [
        {
            "filename": "act.pdf",
            "text": "Акт сверки за январь 2026 по договору",
        }
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Акт",
        action_line="Проверить",
        attachments=attachments,
        mail_type="ACT_RECONCILIATION",
    )

    assert "📎 Акт сверки · январь 2026" in rendered
    assert "таблиц" not in rendered.lower()


def test_attachment_insight_generic_excel_headers() -> None:
    attachments = [
        {
            "filename": "domains.xlsx",
            "text": "URL Created Expires\nexample.com 2026-01-01 2027-01-01",
        }
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Файл",
        action_line="Проверить",
        attachments=attachments,
        mail_type="",
    )

    assert "📎 domains.xlsx — URL / Created / Expires" in rendered


def test_attachment_insight_honest_fallback_for_unreadable_file() -> None:
    attachments = [
        {"filename": "Счет.xls", "text": "@@@ ###"},
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Вложение",
        action_line="Проверить",
        attachments=attachments,
        mail_type="",
    )

    assert "📎 1 вложение: Счет.xls" in rendered
    assert "\n\n\n" not in rendered


def test_attachment_insight_does_not_hallucinate_invoice_or_act() -> None:
    attachments = [
        {"filename": "note.txt", "text": "просто текст без фактов"},
    ]

    rendered = tg_renderer.build_telegram_text(
        priority="🔵",
        from_email="sender@example.com",
        subject="Заметка",
        action_line="Проверить",
        attachments=attachments,
        mail_type="STATUS_UPDATE",
    )

    assert "💰" not in rendered
    assert "Акт сверки" not in rendered
    assert "📎 Счёт" not in rendered


def test_tg_render_premium_human_first_layout() -> None:
    rendered = tg_renderer.render_telegram_message(
        priority="🟡",
        from_email="sender@example.com",
        subject="Тема письма",
        action_line="Ответить",
        summary="""Первая строка
Вторая строка
Третья строка
Четвертая строка""",
        attachments=[{"filename": "invoice.pdf", "text": ""}],
    )

    assert rendered.splitlines()[0] == "🟡 от sender@example.com:"
    assert "<b>Тема письма</b>" in rendered
    assert "<b><i>Ответить</i></b>" in rendered
    assert "📎 1 вложение: invoice.pdf" in rendered
    assert "Первая строка" in rendered and "Четвертая строка" not in rendered
    assert "<i>Powered by LetterBot.ru</i>" in rendered


def test_tg_render_hides_internal_trace_noise_in_default_ux() -> None:
    rendered = tg_renderer.render_telegram_message(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="ATTENTION_GATE Ответить",
        summary="DecisionTraceV1\nLLM_GATE\nКоды: A1\nКонтрфакты",
        attachments=[],
    )

    assert "DecisionTraceV1" not in rendered
    assert "ATTENTION_GATE" not in rendered
    assert "LLM_GATE" not in rendered
    assert "Коды:" not in rendered
    assert "Контрфакты" not in rendered


def test_tg_render_strips_external_mail_warning_tail_from_excerpt() -> None:
    rendered = tg_renderer.render_telegram_message(
        priority="🔵",
        from_email="sender@example.com",
        subject="Subject",
        action_line="Проверить",
        summary="Полезный текст\nВНЕШНЯЯ ПОЧТА: Если отправитель почты неизвестен...\nШум",
        attachments=[],
    )

    assert "Полезный текст" in rendered
    assert "ВНЕШНЯЯ ПОЧТА" not in rendered
    assert "Если отправитель почты неизвестен" not in rendered
