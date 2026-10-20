"""WP7a: CONSTITUTION Appendix A emoji closed list (decision D2).

Only 🔴🟡🔵 ⚡💬⏸️ 📎 💰📄⏰⚠️📦📧 may reach Telegram. The whitelist must catch
every pictographic block, the send boundary strips anything else from text
and button labels, and every Telegram-producing builder is clean at source.
"""

from __future__ import annotations

import pytest

from mailbot_v26.ui.emoji_whitelist import (
    ALLOWED_EMOJIS,
    find_disallowed_emojis,
    strip_disallowed_emojis,
)

APPENDIX_A = "🔴🟡🔵⚡💬⏸️📎💰📄⏰⚠️📦📧"

OFF_LIST_SAMPLES = [
    "🟥",  # U+1F7E5 geometric shapes extended
    "🟨",
    "🟦",
    "🟢",
    "🟠",
    "🔹",  # dropped from the whitelist (D2)
    "🀄",  # U+1F004 mahjong
    "🃏",  # U+1F0CF playing cards
    "🆗",  # U+1F197 enclosed alphanumeric supplement
    "🈯",  # U+1F22F enclosed ideographic supplement
    "🇷🇺",  # regional indicator flag
    "⬆️",  # U+2B06 misc symbols and arrows
    "⭐",  # U+2B50
    "⬛",  # U+2B1B
    "ℹ️",  # text symbol forced to emoji presentation
    "↩️",
    "©️",
    "1️⃣",  # keycap sequence
    "#️⃣",
    "👍🏽",  # skin tone modifier
    "👨‍💻",  # ZWJ sequence
    "🏴󠁧󠁢󠁥󠁮󠁧󠁿",  # tag sequence
    "✓",  # dingbats
    "✗",
    "📊",
    "📈",
    "📌",
    "🚨",
    "💡",
    "🎯",
    "🧠",  # U+1F9E0 supplemental symbols
    "🫠",  # U+1FAE0 symbols and pictographs extended-A
]

_LEFTOVER_MARKS = ("️", "⃣", "‍")


def test_appendix_a_is_the_whole_whitelist() -> None:
    assert "".join(ALLOWED_EMOJIS) == APPENDIX_A
    assert "🔹" not in ALLOWED_EMOJIS


def test_appendix_a_emojis_survive_strip() -> None:
    text = f"Приоритет {APPENDIX_A} и ⚠ ⏸ без VS16"

    assert strip_disallowed_emojis(text) == text
    assert find_disallowed_emojis(text) == set()


@pytest.mark.parametrize("emoji", OFF_LIST_SAMPLES)
def test_off_list_emoji_is_detected_and_stripped(emoji: str) -> None:
    text = f"Счёт {emoji} оплачен"

    cleaned = strip_disallowed_emojis(text)

    assert find_disallowed_emojis(text), emoji
    assert find_disallowed_emojis(cleaned) == set()
    assert cleaned == "Счёт оплачен"
    assert not any(mark in cleaned for mark in _LEFTOVER_MARKS)
    assert not any(0xE0020 <= ord(char) <= 0xE007F for char in cleaned)


def test_plain_text_symbols_are_kept() -> None:
    text = "Счёт №42 → оплатить · ©2026 ™ ◀ Скрыть ▌ «ок» — 5×3 • пункт"

    assert strip_disallowed_emojis(text) == text
    assert find_disallowed_emojis(text) == set()


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("📊 Weekly", "Weekly"),
        ("<b>📊 Title</b>", "<b>Title</b>"),
        ("a 🚀 b", "a b"),
        ("x🚀y", "xy"),
        ("Line\n📊 Next", "Line\nNext"),
        ("🔴🚀 Срочно", "🔴 Срочно"),
        ("🚀🔴 Срочно", "🔴 Срочно"),
        ("🔴️ ok", "🔴 ok"),
        ("✓ Paid", "Paid"),
    ],
)
def test_strip_removes_the_gap_left_by_a_leading_emoji(text: str, expected: str) -> None:
    assert strip_disallowed_emojis(text) == expected


def test_find_reports_each_off_list_sequence() -> None:
    assert find_disallowed_emojis("ok 🚀⌛ 👨‍💻 🔴") == {"🚀", "⌛", "👨‍💻"}


# --- send boundary: send_telegram / edit_telegram_message / inbound reply ---


class _OkResponse:
    status_code = 200
    text = '{"ok": true}'

    def json(self) -> dict[str, object]:
        return {"ok": True, "result": {"message_id": 7}}


class _ParseErrorResponse:
    status_code = 400
    text = "Bad Request: can't parse entities"

    def json(self) -> dict[str, object]:
        return {"ok": False}


def _markup_with_off_list_buttons() -> dict[str, object]:
    return {
        "inline_keyboard": [
            [
                {"text": "🟥▌High", "callback_data": "prio_set:1:R"},
                {"text": "🔴 High", "callback_data": "prio_set:1:R"},
            ],
            [{"text": "✓ Paid", "callback_data": "mb:fb:paid:1"}],
        ]
    }


def _button_texts(markup: dict[str, object]) -> list[str]:
    return [button["text"] for row in markup["inline_keyboard"] for button in row]


def _capture_posts(monkeypatch, responses: list[object] | None = None) -> list[dict]:
    from mailbot_v26.worker import telegram_sender

    calls: list[dict[str, object]] = []

    def _post(**kwargs):
        calls.append(kwargs)
        return responses.pop(0) if responses else _OkResponse()

    monkeypatch.setattr(telegram_sender, "_post_message", _post)
    return calls


def _payload(text: str, markup: dict[str, object] | None = None):
    from mailbot_v26.pipeline.telegram_payload import TelegramPayload

    return TelegramPayload(
        html_text=text,
        priority="🔵",
        metadata={"bot_token": "token", "chat_id": "chat"},
        reply_markup=markup,
    )


def test_send_telegram_strips_off_list_emoji_from_text_and_buttons(
    monkeypatch, caplog
) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = _markup_with_off_list_buttons()
    caplog.set_level("WARNING")

    result = telegram_sender.send_telegram(_payload("🔴 Счёт 🚀\n📊 Итоги", markup))

    assert result.delivered is True
    assert calls[0]["text"] == "🔴 Счёт \nИтоги"
    assert _button_texts(calls[0]["reply_markup"]) == ["▌High", "🔴 High", "Paid"]
    first_button = calls[0]["reply_markup"]["inline_keyboard"][0][0]
    assert first_button["callback_data"] == "prio_set:1:R"
    # The caller's markup is not mutated.
    assert _button_texts(markup) == ["🟥▌High", "🔴 High", "✓ Paid"]
    assert "telegram_emoji_stripped" in caplog.text


def test_send_telegram_keeps_clean_payload_untouched(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = {"inline_keyboard": [[{"text": "🔴 High", "callback_data": "x"}]]}

    telegram_sender.send_telegram(_payload("🔴💰 Счёт 📎", markup))

    assert calls[0]["text"] == "🔴💰 Счёт 📎"
    assert calls[0]["reply_markup"] is markup


def test_send_telegram_plain_salvage_keeps_the_guard(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch, [_ParseErrorResponse(), _OkResponse()])

    result = telegram_sender.send_telegram(
        _payload("<b>📈 Отчёт</b>", _markup_with_off_list_buttons())
    )

    assert result.mode == "plain_salvage"
    assert calls[1]["text"] == "Отчёт"
    labels = " ".join(_button_texts(calls[1]["reply_markup"]))
    assert find_disallowed_emojis(labels) == set()


def test_edit_telegram_message_strips_off_list_emoji(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    posted: list[dict[str, object]] = []

    def _post(_url, json=None, timeout=None):
        posted.append(json)
        return _OkResponse()

    monkeypatch.setattr(telegram_sender.requests, "post", _post)

    applied = telegram_sender.edit_telegram_message(
        bot_token="token",
        chat_id="chat",
        message_id=5,
        html_text="🟡 Обновлено 🔁",
        reply_markup=_markup_with_off_list_buttons(),
    )

    assert applied is True
    assert posted[0]["text"] == "🟡 Обновлено "
    labels = " ".join(_button_texts(posted[0]["reply_markup"]))
    assert find_disallowed_emojis(labels) == set()


def test_inbound_send_message_goes_through_the_guard(monkeypatch) -> None:
    from mailbot_v26.telegram.inbound import TelegramInboundClient

    calls = _capture_posts(monkeypatch)
    client = TelegramInboundClient(bot_token="token", _requests=object())

    result = client.send_message(chat_id="chat", text="📌 Обязательства 🎯")

    assert result.delivered is True
    assert find_disallowed_emojis(calls[0]["text"]) == set()
    assert "Обязательства" in calls[0]["text"]


def test_callback_toast_goes_through_the_guard(monkeypatch) -> None:
    from types import SimpleNamespace

    from mailbot_v26.telegram.inbound import TelegramInboundProcessor
    from mailbot_v26.worker import telegram_sender

    posted: list[dict[str, object]] = []

    def _post(_url, json=None, timeout=None):
        posted.append(json)
        return _OkResponse()

    monkeypatch.setattr(telegram_sender.requests, "post", _post)

    TelegramInboundProcessor._answer_callback(
        SimpleNamespace(bot_token="token"), "cb-1", "✓ Приоритет 🟥 → 🔴"
    )

    assert posted[0]["text"] == "Приоритет → 🔴"


def test_button_made_only_of_off_list_emoji_keeps_a_label(monkeypatch) -> None:
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    markup = {"inline_keyboard": [[{"text": "🚀", "callback_data": "x"}]]}

    telegram_sender.send_telegram(_payload("ok", markup))

    assert _button_texts(calls[0]["reply_markup"]) == ["•"]


# --- every Telegram-producing builder is clean at the source ---------------

LOCALES = ("ru", "en")


def _assert_clean(text: str) -> None:
    assert find_disallowed_emojis(text) == set(), text


def _assert_clean_markup(markup: dict[str, object] | None) -> None:
    if markup is None:
        return
    labels = _button_texts(markup)
    assert labels
    for label in labels:
        _assert_clean(label)


@pytest.mark.parametrize(
    "request_name",
    ["_full_request", "_short_request", "_fallback_request", "_premium_request"],
)
def test_email_notification_and_keyboard_are_clean(request_name: str) -> None:
    from mailbot_v26.telegram.render_contract import render_email_notification
    from mailbot_v26.tests import test_telegram_render as render_cases

    result = render_email_notification(getattr(render_cases, request_name)())

    _assert_clean(result.text)
    _assert_clean_markup(result.reply_markup)


def test_snapshot_fixtures_are_clean() -> None:
    from pathlib import Path

    fixtures = Path(__file__).resolve().parent / "fixtures" / "telegram_snapshots"
    paths = sorted(fixtures.glob("*.txt"))

    assert len(paths) == 4
    for path in paths:
        _assert_clean(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("locale", LOCALES)
def test_progressive_minimal_tier1_is_clean(monkeypatch, locale: str) -> None:
    from mailbot_v26.pipeline import processor

    monkeypatch.setattr(processor, "_resolve_outbound_ui_locale", lambda: locale)

    payload = processor._build_progressive_telegram_payload(priority="🔵", metadata={})

    _assert_clean(payload.html_text)
    assert payload.html_text.startswith("📧 ")


def test_signal_hints_are_clean() -> None:
    from mailbot_v26.insights.aggregator import Insight
    from mailbot_v26.pipeline import processor

    hints = processor._build_signal_hints(
        [
            Insight(
                type="silence",
                severity="MEDIUM",
                explanation="Контакт молчит",
                recommendation="",
            ),
            Insight(
                type="deadlock",
                severity="MEDIUM",
                explanation="Без ответа 5 дней",
                recommendation="",
            ),
        ]
    )

    assert len(hints) == 2
    for hint in hints:
        _assert_clean(hint)


@pytest.mark.parametrize("locale", LOCALES)
def test_relationship_block_is_clean(locale: str) -> None:
    from mailbot_v26.pipeline import tg_renderer

    text = tg_renderer.render_telegram_message(
        priority="🔴",
        from_email="billing@vector.example",
        subject="Счёт",
        action_line="Оплатить",
        summary="Счёт на оплату",
        attachments=[],
        locale=locale,
        relationship_profile={"emails_count": 25, "invoice_count": 4, "overdue_count": 2},
    )

    assert ("Relationship:" if locale == "en" else "Контрагент:") in text
    _assert_clean(text)


@pytest.mark.parametrize("doc_kind", ["invoice", "payroll", "contract", "reconciliation", ""])
@pytest.mark.parametrize("locale", LOCALES)
def test_notification_keyboards_are_clean(doc_kind: str, locale: str) -> None:
    from mailbot_v26.telegram.keyboard_builder import build_notification_keyboard

    markup = build_notification_keyboard(
        render_mode="full",
        doc_kind=doc_kind,
        priority="🔴",
        message_key=42,
        show_decision_trace=True,
        locale=locale,
    )

    _assert_clean_markup(markup)


@pytest.mark.parametrize("locale", LOCALES)
def test_email_action_keyboards_are_clean(locale: str) -> None:
    from mailbot_v26.telegram.decision_trace_ui import (
        build_decision_trace_keyboard,
        build_email_actions_keyboard,
    )

    for kwargs in ({}, {"prio_menu": True}, {"snooze_menu": True}):
        _assert_clean_markup(
            build_email_actions_keyboard(
                email_id=42, expanded=False, locale=locale, **kwargs
            )
        )
    for expanded in (False, True):
        _assert_clean_markup(
            build_decision_trace_keyboard(email_id=42, expanded=expanded, locale=locale)
        )


def test_priority_buttons_use_appendix_a_circles() -> None:
    from mailbot_v26.telegram.decision_trace_ui import build_email_actions_keyboard
    from mailbot_v26.telegram.keyboard_builder import build_notification_keyboard

    expected = ["🔵 Low", "🟡 Medium", "🔴 High"]
    actions = build_email_actions_keyboard(email_id=1, expanded=False, prio_menu=True)
    notification = build_notification_keyboard(
        render_mode="full", doc_kind="", priority="🔴", message_key=1
    )

    assert _button_texts(actions) == expected
    assert _button_texts(notification)[:3] == expected


def _digest_data(**overrides):
    from mailbot_v26.insights.attention_economics import (
        AttentionEconomicsResult,
        AttentionEntity,
    )
    from mailbot_v26.pipeline import daily_digest

    entity = AttentionEntity(
        entity_id="e1",
        label="ООО Вектор",
        message_count=5,
        attachment_count=1,
        estimated_read_minutes=12.5,
        deferred_count=1,
        trust_delta=0.05,
        health_delta=2.0,
    )
    attention = AttentionEconomicsResult(
        window_days=7,
        sample_size=20,
        entities=[entity],
        top_sinks=[entity],
        at_risk=[entity],
        best_counterparties=[entity],
    )
    base = dict(
        deferred_total=2,
        deferred_attachments_only=1,
        deferred_informational=1,
        deferred_items=[],
        uncertainty_queue_items=[],
        commitments_pending=1,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=["Всплеск писем"],
        attention_economics=attention,
        quality_metrics=None,
        notification_sla=None,
        deadlock_insights=[{"from_email": "boss@example.com", "subject": "Счёт"}],
        silence_insights=[{"contact": "client@example.com", "days_silent": 5}],
        digest_insights_enabled=True,
        digest_insights_max_items=5,
        digest_action_templates_enabled=False,
        behavior_metrics={"surprise_rate": 0.2, "corrections": 3},
        behavior_metrics_enabled=True,
    )
    base.update(overrides)
    return daily_digest.DigestData(**base)


@pytest.mark.parametrize("locale", LOCALES)
@pytest.mark.parametrize("learning_mode", [False, True])
def test_daily_digest_is_clean(locale: str, learning_mode: bool) -> None:
    from mailbot_v26.pipeline import daily_digest

    overrides = {}
    if learning_mode:
        overrides["trust_bootstrap_snapshot"] = daily_digest.TrustBootstrapSnapshot(
            start_ts=1.0,
            days_since_start=1.0,
            samples_count=12,
            corrections_count=0,
            surprises_count=0,
            surprise_rate=None,
            active=True,
        )
        overrides["trust_bootstrap_min_samples"] = 50

    text = daily_digest._build_digest_text(_digest_data(**overrides), locale=locale)

    assert "<b>" in text
    _assert_clean(text)


@pytest.mark.parametrize("locale", LOCALES)
def test_weekly_digest_card_and_attention_block_are_clean(locale: str) -> None:
    from mailbot_v26.pipeline import weekly_digest

    data = weekly_digest.WeeklyDigestData(
        week_key="2025-W02",
        total_emails=47,
        deferred_emails=3,
        attention_entities=(),
        commitment_counts={},
        overdue_commitments=(),
        trust_deltas={"up": [], "down": []},
        anomaly_alerts=["Всплеск писем"],
        attention_economics=_digest_data().attention_economics,
        invoice_count=3,
        invoice_total_rub=387000,
        contract_count=2,
    )

    card = weekly_digest._build_shareable_weekly_card(data, locale=locale)

    _assert_clean(weekly_digest._build_weekly_digest_text(data, locale=locale))
    _assert_clean(card)
    _assert_clean_markup(weekly_digest._build_shareable_card_keyboard(card, locale))
    _assert_clean(
        "\n".join(
            weekly_digest._format_attention_block_localized(
                data.attention_economics, locale
            )
        )
    )


def test_attention_block_and_reliability_labels_are_clean() -> None:
    from mailbot_v26.insights.attention_economics import format_attention_block
    from mailbot_v26.insights.commitment_signals import _label_for_score

    _assert_clean("\n".join(format_attention_block(_digest_data().attention_economics)))
    for score in (95, 60, 10):
        _assert_clean(_label_for_score(score))


def test_support_footer_and_watermark_are_clean() -> None:
    from mailbot_v26.pipeline.digest_scheduler import (
        SupportTelegramConfig,
        _render_support_footer,
    )
    from mailbot_v26.ui import branding

    footer = _render_support_footer(
        SupportTelegramConfig(
            enabled=True,
            telegram=True,
            min_days_between_asks=30,
            url="https://example.com/support",
            message="Поддержать LetterBot.ru: {url}",
        )
    )

    assert footer.startswith("Поддержать LetterBot.ru")
    _assert_clean(footer)
    _assert_clean(branding.WATERMARK_LINE)
    _assert_clean(branding.WATERMARK_HTML_LINE)


@pytest.mark.parametrize("locale", LOCALES)
def test_mailbox_health_alert_is_clean(tmp_path, locale: str) -> None:
    from datetime import datetime, timezone

    from mailbot_v26.config_loader import AccountConfig
    from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager

    manager = AccountRuntimeHealthManager(
        tmp_path / "runtime_health.json", locale_resolver=lambda: locale
    )
    manager.register_account(
        AccountConfig(
            account_id="acc",
            login="alice@example.com",
            password="pass",
            host="imap.example.com",
            port=993,
            use_ssl=True,
            telegram_chat_id="chat",
        )
    )

    _alert, text = manager.on_failure(
        "acc", TimeoutError("timed out"), datetime(2026, 1, 1, tzinfo=timezone.utc)
    )

    assert text.startswith("⚠️ ")
    _assert_clean(text)


@pytest.mark.parametrize("locale", LOCALES)
def test_dead_letter_alert_is_clean(locale: str) -> None:
    from mailbot_v26 import start as start_module

    text = start_module._dead_letter_alert_text(
        locale=locale, masked_login="a***@example.com", count=2, context_count=1
    )

    _assert_clean(text)


def test_account_login_failure_alert_is_clean_after_the_guard(monkeypatch) -> None:
    from mailbot_v26.health.mail_accounts import (
        MailAccountHealth,
        format_account_failure_message,
    )
    from mailbot_v26.worker import telegram_sender

    calls = _capture_posts(monkeypatch)
    text = format_account_failure_message(
        MailAccountHealth(
            account_id="acc",
            host="imap.example.com",
            status="FAIL",
            error="auth failed",
        )
    )

    telegram_sender.send_telegram(_payload(text))

    _assert_clean(calls[0]["text"])
    assert calls[0]["text"].startswith("ACCOUNT LOGIN FAILED")


def _inbound_processor(tmp_path, sent: list[str], locale: str):
    from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
    from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
    from mailbot_v26.features.flags import FeatureFlags
    from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
    from mailbot_v26.observability.event_emitter import EventEmitter
    from mailbot_v26.storage.analytics import KnowledgeAnalytics
    from mailbot_v26.storage.knowledge_db import KnowledgeDB
    from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
    from mailbot_v26.telegram.inbound import TelegramInboundProcessor
    from mailbot_v26.worker.telegram_sender import DeliveryResult

    db_path = tmp_path / "knowledge.sqlite"

    def _send_reply(_chat_id: str, text: str) -> DeliveryResult:
        sent.append(text)
        return DeliveryResult(delivered=True, retryable=False)

    processor = TelegramInboundProcessor(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        event_emitter=EventEmitter(tmp_path / "events.sqlite"),
        contract_event_emitter=ContractEventEmitter(db_path),
        runtime_flag_store=RuntimeFlagStore(path=tmp_path / "runtime_flags.json"),
        auto_priority_gate=object(),
        auto_priority_gate_config=AutoPriorityGateConfig(enabled=False),
        override_store=RuntimeOverrideStore(db_path),
        send_reply=_send_reply,
        feature_flags=FeatureFlags(base_dir=tmp_path),
        allowed_chat_ids=frozenset({"chat"}),
        bot_token="token",
        locale=locale,
        chat_account_emails={"chat": ("alice@example.com",)},
    )
    processor.override_store.set_insider_since("2026-02", chat_id="chat")
    return processor


def _seed_commitment(db_path) -> None:
    import sqlite3
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO emails (account_email, from_email, subject, received_at,"
            " priority, action_line, body_summary, raw_body_hash)"
            " VALUES ('alice@example.com', 's@example.com', 'S', ?, '🔴', 'Ответить',"
            " '', 'h')",
            (now,),
        )
        conn.execute(
            "INSERT INTO commitments (email_row_id, source, commitment_text,"
            " deadline_iso, status, confidence, created_at)"
            " VALUES (?, 'llm', 'Отправить акт', NULL, 'pending', 0.9, ?)",
            (cursor.lastrowid, now),
        )
        conn.commit()


@pytest.mark.parametrize("locale", LOCALES)
def test_inbound_command_replies_are_clean(tmp_path, monkeypatch, locale: str) -> None:
    monkeypatch.setattr(
        "mailbot_v26.pipeline.processor.configure_processor_locale",
        lambda _locale: None,
    )
    sent: list[str] = []
    processor = _inbound_processor(tmp_path, sent, locale)
    _seed_commitment(processor.knowledge_db.path)
    commands = [
        "/help",
        "/status",
        "/commitments",
        "/week",
        "/stats",
        "/support",
        "/digest off",
        "/autopriority off",
        f"/lang {locale}",
    ]

    for command in commands:
        processor.handle_message({"chat": {"id": "chat"}, "text": command})

    assert len(sent) == len(commands)
    assert any("Insider since" in reply for reply in sent)
    for reply in sent:
        _assert_clean(reply)


@pytest.mark.parametrize("locale", LOCALES)
def test_snooze_reminder_is_clean(tmp_path, monkeypatch, locale: str) -> None:
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    from mailbot_v26 import start as start_module
    from mailbot_v26.bot_core.storage import Storage
    from mailbot_v26.config_loader import (
        AccountConfig,
        BotConfig,
        GeneralConfig,
        KeysConfig,
        StorageConfig,
    )
    from mailbot_v26.pipeline import processor
    from mailbot_v26.worker.telegram_sender import DeliveryResult

    sent: list[object] = []
    monkeypatch.setattr(
        start_module,
        "send_telegram",
        lambda payload: sent.append(payload)
        or DeliveryResult(delivered=True, retryable=False, message_id=1),
    )
    monkeypatch.setattr(
        start_module,
        "load_telegram_ui_config",
        lambda *_a, **_k: SimpleNamespace(show_decision_trace=False),
    )
    monkeypatch.setattr(processor, "_resolve_outbound_ui_locale", lambda: locale)
    storage = Storage(tmp_path / "mailbot.sqlite")
    email_id = storage.upsert_email(
        account_email="alice@example.com",
        uid=1,
        message_id="<m1@example.com>",
        from_email="s@example.com",
        from_name="S",
        subject="Счёт",
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
    )
    due = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    with storage.conn:
        storage.conn.execute(
            "INSERT INTO telegram_snooze (email_id, deliver_at_utc, snoozed_at_utc,"
            " status, reminder_text, attempts, created_at, updated_at)"
            " VALUES (?, ?, ?, 'pending', '', 0, ?, ?)",
            (email_id, due, due, due, due),
        )
    config = BotConfig(
        general=GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50000,
            max_extracted_total_chars=120000,
            admin_chat_id="",
        ),
        accounts=[
            AccountConfig(
                account_id="acc",
                login="alice@example.com",
                password="pass",
                host="imap.example.com",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            )
        ],
        keys=KeysConfig(telegram_bot_token="token", cf_account_id="", cf_api_token=""),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )
    try:
        start_module._process_due_snoozes(storage=storage, config=config)
    finally:
        storage.close()

    assert len(sent) == 1
    assert sent[0].html_text.startswith("⏰ ")
    _assert_clean(sent[0].html_text)
    _assert_clean_markup(sent[0].reply_markup)
