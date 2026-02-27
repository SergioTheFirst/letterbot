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

    assert "Вложения: 1 (PDF×1)" in rendered
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
