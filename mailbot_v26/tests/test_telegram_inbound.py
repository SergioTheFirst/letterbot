from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.config_loader import SupportSettings
from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
from mailbot_v26.version import __version__
from mailbot_v26.telegram.inbound import (
    InboundStateStore,
    TelegramInboundProcessor,
    parse_callback_data,
    parse_command,
    run_inbound_polling,
)
from mailbot_v26.telegram.decision_trace_ui import build_decision_trace_keyboard
from mailbot_v26.worker.telegram_sender import DeliveryResult


@dataclass
class StubGate:
    result: GateResult

    def evaluate(self, **_kwargs) -> GateResult:
        return self.result


def _insert_email(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                account_email,
                from_email,
                subject,
                received_at,
                priority,
                action_line,
                body_summary,
                raw_body_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "account@example.com",
                "sender@example.com",
                "Subject",
                datetime.now(timezone.utc).isoformat(),
                "🔵",
                "Проверить",
                "",
                "hash",
            ),
        )
        row = conn.execute("SELECT id FROM emails ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    return int(row[0])


def _build_processor(tmp_path: Path, sent: list[str], gate_result: GateResult) -> TelegramInboundProcessor:
    db_path = tmp_path / "knowledge.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    analytics = KnowledgeAnalytics(db_path)
    event_emitter = EventEmitter(tmp_path / "events.sqlite")
    contract_emitter = ContractEventEmitter(db_path)
    runtime_flags = RuntimeFlagStore(path=tmp_path / "runtime_flags.json")
    override_store = RuntimeOverrideStore(db_path)
    feature_flags = FeatureFlags(base_dir=tmp_path)

    def _send_reply(_chat_id: str, text: str) -> DeliveryResult:
        sent.append(text)
        return DeliveryResult(delivered=True, retryable=False)

    return TelegramInboundProcessor(
        knowledge_db=knowledge_db,
        analytics=analytics,
        event_emitter=event_emitter,
        contract_event_emitter=contract_emitter,
        runtime_flag_store=runtime_flags,
        auto_priority_gate=StubGate(gate_result),
        auto_priority_gate_config=AutoPriorityGateConfig(enabled=True),
        override_store=override_store,
        send_reply=_send_reply,
        feature_flags=feature_flags,
        allowed_chat_ids=frozenset({"chat"}),
        bot_token="token",
    )


def test_parse_callback_data_priority() -> None:
    parsed = parse_callback_data("mb:prio:123:R")
    assert parsed == ("priority", {"email_id": "123", "priority": "🔴"})
    parsed = parse_callback_data("prio:55:🔵")
    assert parsed == ("priority", {"email_id": "55", "priority": "🔵"})
    parsed = parse_callback_data("mb:help:priority")
    assert parsed == ("help", {"topic": "priority"})
    parsed = parse_callback_data("mb:d:42")
    assert parsed == ("details", {"email_id": "42"})
    parsed = parse_callback_data("mb:h:7")
    assert parsed == ("hide", {"email_id": "7"})
    parsed = parse_callback_data("mb:ok:11")
    assert parsed == ("priority_ok", {"email_id": "11"})
    assert parse_callback_data("mb:prio:bad") is None
    assert parse_callback_data("mb:prio::R") is None


def test_decision_trace_callback_length() -> None:
    keyboard = build_decision_trace_keyboard(email_id=123456, expanded=False)
    callback = keyboard["inline_keyboard"][0][0]["callback_data"]
    assert len(callback.encode("utf-8")) <= 64


def test_parse_command_tolerates_spaces() -> None:
    command, args = parse_command("  /digest   on ")
    assert command == "/digest"
    assert args == ["on"]


def test_priority_correction_deduped(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=1,
        correction_rate=0.01,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    email_id = _insert_email(processor.knowledge_db.path)

    callback = {
        "data": f"mb:prio:{email_id}:R",
        "message": {"chat": {"id": "chat"}},
    }
    processor.handle_callback_query(callback)
    processor.handle_callback_query(callback)

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        feedback_rows = conn.execute("SELECT id FROM priority_feedback").fetchall()
        event_rows = conn.execute(
            "SELECT event_type FROM events_v1 WHERE event_type = 'priority_correction_recorded'"
        ).fetchall()

    assert len(feedback_rows) == 1
    assert len(event_rows) == 1
    assert any("Принято: приоритет исправлен на 🔴" in text for text in sent)


def test_digest_toggle_command_updates_override(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=100,
        corrections=1,
        correction_rate=0.01,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/digest on"})
    overrides = processor.override_store.get_overrides()

    assert overrides.digest_enabled is True
    assert sent[-1] == "Дайджесты включены."


def test_autopriority_toggle_respects_gate(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=False,
        reason="insufficient_samples",
        window_days=30,
        samples=0,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/autopriority on"})
    flags, _ = processor.runtime_flag_store.get_flags(force=True)

    assert flags.enable_auto_priority is False
    assert any("Пока нельзя" in text for text in sent)


def test_run_inbound_polling_updates_offset(tmp_path: Path) -> None:
    state_store = InboundStateStore(tmp_path / "state.sqlite")

    class StubClient:
        def __init__(self, updates: list[dict[str, object]]) -> None:
            self._updates = updates

        def get_updates(self, **_kwargs):
            return self._updates

    class StubProcessor:
        def __init__(self) -> None:
            self.seen: list[dict[str, object]] = []

        def handle_update(self, update: dict[str, object]) -> None:
            self.seen.append(update)

    updates = [{"update_id": 10, "message": {"chat": {"id": "chat"}, "text": "/help"}}]
    processor = StubProcessor()
    run_inbound_polling(
        client=StubClient(updates),
        processor=processor,
        state_store=state_store,
    )

    assert state_store.get_last_update_id() == 10
    assert processor.seen == updates


def test_run_inbound_polling_survives_errors(tmp_path: Path) -> None:
    state_store = InboundStateStore(tmp_path / "state.sqlite")

    class StubClient:
        def get_updates(self, **_kwargs):
            return [{"update_id": 11, "message": {"chat": {"id": "chat"}, "text": "/help"}}]

    class StubProcessor:
        def handle_update(self, update: dict[str, object]) -> None:
            raise RuntimeError("boom")

    run_inbound_polling(
        client=StubClient(),
        processor=StubProcessor(),
        state_store=state_store,
    )

    assert state_store.get_last_update_id() == 11


def test_snooze_callback_creates_pending_record(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    email_id = _insert_email(processor.knowledge_db.path)

    processor.handle_callback_query(
        {
            "id": "cb1",
            "data": f"snz_s:{email_id}:2h",
            "message": {"chat": {"id": "chat"}, "message_id": 10, "text": "msg"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        row = conn.execute(
            "SELECT email_id, status FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == email_id
    assert row[1] == "pending"


def test_commitments_command_empty(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/commitments"})

    assert sent[-1] == "✅ Нет открытых обязательств"


def test_tasks_alias_and_limit(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    email_id = _insert_email(processor.knowledge_db.path)
    with sqlite3.connect(processor.knowledge_db.path) as conn:
        for idx in range(10):
            conn.execute(
                """
                INSERT INTO commitments (email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at)
                VALUES (?, 'llm', ?, ?, 'pending', 0.9, ?)
                """,
                (
                    email_id,
                    f"Задача {idx}",
                    f"2026-03-{idx+1:02d}T10:00:00+00:00",
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        conn.commit()

    processor.handle_message({"chat": {"id": "chat"}, "text": "/tasks"})

    output = sent[-1]
    assert output.startswith("📋 <b>Обязательства:</b>")
    assert output.count("\n• ") == 7


def test_priority_ok_callback_records_positive_feedback(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    email_id = _insert_email(processor.knowledge_db.path)

    processor.handle_callback_query(
        {
            "id": "cb-ok",
            "data": f"mb:ok:{email_id}",
            "message": {"chat": {"id": "chat"}, "message_id": 10, "text": "msg"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        rows = conn.execute(
            "SELECT kind, value FROM priority_feedback WHERE email_id = ?",
            (str(email_id),),
        ).fetchall()
    assert rows == [("priority_confirmation", "🔵")]


def test_priority_ok_callback_graceful_on_missing_email(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_callback_query(
        {
            "id": "cb-ok-missing",
            "data": "mb:ok:99999",
            "message": {"chat": {"id": "chat"}, "message_id": 10, "text": "msg"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM priority_feedback").fetchone()[0]
    assert count == 0


def test_week_command_returns_compact_summary_with_empty_dataset(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/week"})

    assert sent[-1] == (
        "📊 Letterbot — неделя\n"
        "Писем: 0 · Важных: 0 · Низких: 0\n"
        "Коррекций: 0 · Точность: н/д\n"
        "Обязательств открыто: 0"
    )


def test_week_command_returns_compact_summary_with_data(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    email_id = _insert_email(processor.knowledge_db.path)
    now_ts = datetime.now(timezone.utc).timestamp()

    processor.contract_event_emitter.emit(
        EventV1(
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=now_ts,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={},
        )
    )
    processor.contract_event_emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=now_ts + 1,
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={"old_priority": "🔵", "new_priority": "🔴"},
        )
    )
    with sqlite3.connect(processor.knowledge_db.path) as conn:
        conn.execute("UPDATE emails SET priority = '🔴' WHERE id = ?", (email_id,))
        conn.execute(
            """
            INSERT INTO commitments (email_row_id, source, commitment_text, deadline_iso, status, confidence, created_at)
            VALUES (?, 'llm', 'Проверить договор', NULL, 'pending', 0.9, ?)
            """,
            (email_id, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

    processor.handle_message({"chat": {"id": "chat"}, "text": "week"})

    assert sent[-1] == (
        "📊 Letterbot — неделя\n"
        "Писем: 1 · Важных: 1 · Низких: 0\n"
        "Коррекций: 1 · Точность: 100%\n"
        "Обязательств открыто: 1"
    )


def test_support_command_disabled_returns_honest_message(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    processor.support_settings = SupportSettings(
        enabled=False,
        text="text",
        url="CHANGE_ME",
        label="Поддержать Letterbot",
        frequency_days=30,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "/support"})

    assert sent[-1] == "Поддержка проекта сейчас не настроена."


def test_support_command_enabled_with_url_returns_three_lines(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    processor.support_settings = SupportSettings(
        enabled=True,
        text="Если Letterbot помогает, проект можно поддержать",
        url="https://example.com/insider",
        label="Поддержать Letterbot",
        frequency_days=30,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "support"})

    assert sent[-1] == (
        "Поддержать Letterbot\n"
        "Если Letterbot помогает, проект можно поддержать\n"
        "https://example.com/insider"
    )


def test_support_command_enabled_without_url_reports_not_configured(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    processor.support_settings = SupportSettings(
        enabled=True,
        text="text",
        url="CHANGE_ME",
        label="Поддержать Letterbot",
        frequency_days=30,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "/support"})

    assert sent[-1] == "Поддержка включена, но ссылка ещё не настроена."


def test_help_contains_support_command(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/help"})

    assert "/support — поддержать проект" in sent[-1]


def test_status_without_insider_badge_by_default(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})

    assert "⭐ Letterbot Insider since:" not in sent[-1]
    assert f"Version: {__version__}" in sent[-1]


def test_status_shows_insider_badge_when_set(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="priority_v2_auto",
    )
    processor = _build_processor(tmp_path, sent, gate_result)
    processor.override_store.set_insider_since("2026-02", chat_id="chat")

    processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})

    assert "⭐ Letterbot Insider since: 2026-02" in sent[-1]


def test_runtime_override_store_insider_roundtrip(tmp_path: Path) -> None:
    store = RuntimeOverrideStore(tmp_path / "runtime.sqlite")

    assert store.get_insider_since(chat_id="chat") is None

    store.set_insider_since("2025-12", chat_id="chat")
    assert store.get_insider_since(chat_id="chat") == "2025-12"

    store.set_insider_since("", chat_id="chat")
    assert store.get_insider_since(chat_id="chat") is None
