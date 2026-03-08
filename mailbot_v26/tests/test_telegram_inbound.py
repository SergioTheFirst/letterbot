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
from mailbot_v26.text.mojibake import normalize_mojibake_text


def _norm(text: str) -> str:
    return normalize_mojibake_text(str(text))


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


def _build_processor(
    tmp_path: Path, sent: list[str], gate_result: GateResult
) -> TelegramInboundProcessor:
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


def test_legacy_trace_callback_does_not_crash_when_hidden_by_default(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )

    callback = {
        "id": "cb-1",
        "data": f"mb:d:{email_id}",
        "message": {"message_id": 55, "chat": {"id": "chat"}},
    }
    processor.handle_callback_query(callback)

    assert edited
    assert "trace not available" in _norm(str(edited[0]["html_text"]))


def test_parse_command_tolerates_spaces() -> None:
    command, args = parse_command("  /digest   on ")
    assert command == "/digest"
    assert args == ["on"]


def test_priority_callback_updates_snapshot_priority(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )

    callback = {
        "data": f"prio_set:{email_id}:R",
        "message": {"chat": {"id": "chat"}, "message_id": 101, "text": "old"},
    }
    processor.handle_callback_query(callback)

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        priority_row = conn.execute(
            "SELECT priority, priority_source FROM emails WHERE id = ?", (email_id,)
        ).fetchone()
        feedback_rows = conn.execute("SELECT id FROM priority_feedback").fetchall()
        event_rows = conn.execute(
            "SELECT event_type FROM events_v1 WHERE event_type = 'priority_correction_recorded'"
        ).fetchall()

    assert priority_row is not None
    assert priority_row == ("\U0001f534", "user_override")
    assert len(feedback_rows) == 1
    assert len(event_rows) == 1
    assert sent == []
    assert len(edited) == 1
    assert "\U0001f534" in _norm(str(edited[0]["html_text"]))
    assert "Принято: приоритет исправлен" not in str(edited[0]["html_text"])
    assert edited[0]["reply_markup"] == {
        "inline_keyboard": [
            [
                {
                    "text": "Изменить приоритет",
                    "callback_data": f"prio_menu:{email_id}",
                },
                {"text": "⏰ Отложить", "callback_data": f"snz_m:{email_id}"},
            ]
        ]
    }
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )

    callback = {
        "data": f"prio_set:{email_id}:R",
        "message": {"chat": {"id": "chat"}, "message_id": 101, "text": "old"},
    }
    processor.handle_callback_query(callback)
    processor.handle_callback_query(callback)

    assert len(edited) == 2
    assert "🔴" in _norm(str(edited[0]["html_text"]))
    assert "🔴" in str(edited[1]["html_text"])


def test_priority_callback_edit_same_message(tmp_path: Path, monkeypatch) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )

    callback = {
        "data": f"mb:prio:{email_id}:Y",
        "message": {"chat": {"id": "chat"}, "message_id": 808, "text": "old"},
    }
    processor.handle_callback_query(callback)

    assert sent == []
    assert len(edited) == 1
    assert edited[0]["chat_id"] == "chat"
    assert edited[0]["message_id"] == 808


def test_priority_callback_edit_failure_is_safe_without_ack_spam(
    tmp_path: Path, monkeypatch
) -> None:
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

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("edit failed")),
    )

    callback_acks: list[dict[str, object]] = []

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            callback_acks.append({"json": json, "timeout": timeout})
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio",
        "data": f"mb:prio:{email_id}:Y",
        "message": {"chat": {"id": "chat"}, "message_id": 88, "text": "old"},
    }
    processor.handle_callback_query(callback)

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        feedback_rows = conn.execute("SELECT id FROM priority_feedback").fetchall()

    assert feedback_rows
    assert sent == []
    assert len(callback_acks) == 1
    assert callback_acks[0]["json"]["callback_query_id"] == "cb-prio"
    assert callback_acks[0]["json"]["show_alert"] is False
    assert callback_acks[0]["timeout"] == 5
    assert _norm(str(callback_acks[0]["json"]["text"])) == _norm(
        "Не могу отредактировать"
    )


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
    assert _norm(sent[-1]) == _norm("Дайджесты включены.")


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

    assert flags.enable_auto_priority is True
    assert any("Пока нельзя" in _norm(text) for text in sent)


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
            return [
                {"update_id": 11, "message": {"chat": {"id": "chat"}, "text": "/help"}}
            ]

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

    assert _norm(sent[-1]) == _norm("✅ Нет открытых обязательств")


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
    assert _norm(output).startswith("📋 <b>Обязательства:</b>")
    assert _norm(output).count("\n• ") == 7


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


def test_week_command_returns_compact_summary_with_empty_dataset(
    tmp_path: Path,
) -> None:
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

    assert _norm("📊 Letterbot — неделя") in _norm(sent[-1])
    assert _norm("Коррекций: 0") in _norm(sent[-1])
    assert _norm("Surprise rate: н/д") in _norm(sent[-1])
    assert _norm("Переходы: нет данных") in _norm(sent[-1])


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

    assert _norm("📊 Letterbot — неделя") in _norm(sent[-1])
    assert _norm("Коррекций: 1 · Точность: 100%") in _norm(sent[-1])
    assert _norm("Surprise rate: 0%") in _norm(sent[-1])
    assert _norm("Переходы: 🔵→🔴 ×1") in _norm(sent[-1])


def test_stats_command_returns_human_friendly_summary(tmp_path: Path) -> None:
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

    callback = {
        "data": f"mb:prio:{email_id}:R",
        "message": {"chat": {"id": "chat"}, "message_id": 77},
    }
    processor.handle_callback_query(callback)

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        feedback_rows = conn.execute("SELECT id FROM priority_feedback").fetchall()
    assert feedback_rows

    processor.handle_message({"chat": {"id": "chat"}, "text": "/stats"})

    assert _norm("📈 Качество автоприоритизации") in _norm(sent[-1])
    assert _norm("Коррекций: 1") in _norm(sent[-1])
    assert _norm("Surprise rate:") in _norm(sent[-1])
    assert _norm("Переходы:") in _norm(sent[-1])
    assert _norm("Можно доверять автоприоритизации:") in _norm(sent[-1])


def test_stats_command_handles_analytics_failures_without_crash(
    tmp_path: Path, monkeypatch
) -> None:
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

    def _boom(*_args, **_kwargs):
        raise RuntimeError("analytics unavailable")

    monkeypatch.setattr(processor.analytics, "weekly_accuracy_report", _boom)
    monkeypatch.setattr(processor.analytics, "weekly_calibration_proposals", _boom)
    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.compute_priority_calibration_report",
        _boom,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "/stats"})

    normalized = _norm(sent[-1])
    assert "📈 Качество автоприоритизации" in normalized
    assert "Коррекций: 0" in normalized
    assert "Surprise rate: н/д" in normalized
    assert "Переходы: нет данных" in normalized
    assert "Пока данных мало — делаем выводы вручную." in normalized


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

    assert _norm(sent[-1]) == _norm("Поддержка проекта сейчас не настроена.")


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

    assert _norm(sent[-1]) == _norm(
        "Поддержать Letterbot\nЕсли Letterbot помогает, проект можно поддержать\nhttps://example.com/insider"
    )


def test_support_command_enabled_without_url_reports_not_configured(
    tmp_path: Path,
) -> None:
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

    assert _norm(sent[-1]) == _norm("Поддержка включена, но ссылка ещё не настроена.")


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

    assert _norm("/support — поддержать проект") in _norm(sent[-1])
    assert _norm("/stats — качество автоприоритизации") in _norm(sent[-1])


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

    assert _norm("РІВ­С’ Letterbot Insider since:") not in _norm(sent[-1])
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

    assert _norm("⭐ Letterbot Insider since: 2026-02") in _norm(sent[-1])


def test_runtime_override_store_insider_roundtrip(tmp_path: Path) -> None:
    store = RuntimeOverrideStore(tmp_path / "runtime.sqlite")

    assert store.get_insider_since(chat_id="chat") is None

    store.set_insider_since("2025-12", chat_id="chat")
    assert store.get_insider_since(chat_id="chat") == "2025-12"

    store.set_insider_since("", chat_id="chat")
    assert store.get_insider_since(chat_id="chat") is None


def test_priority_callback_edits_same_message_and_updates_priority_text(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    callback_acks: list[dict[str, object]] = []

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            callback_acks.append({"json": json, "timeout": timeout})
            return object()

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )
    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-edit",
        "data": f"prio_set:{email_id}:Y",
        "message": {"chat": {"id": "chat"}, "message_id": 202, "text": "old"},
    }
    processor.handle_callback_query(callback)

    assert len(edited) == 1
    assert edited[0]["chat_id"] == "chat"
    assert edited[0]["message_id"] == 202
    assert "🟡" in _norm(str(edited[0]["html_text"]))
    assert sent == []
    assert callback_acks and _norm(callback_acks[0]["json"]["text"]) == _norm(
        "Приоритет обновлён"
    )


def test_priority_callback_always_answers_callback_query(
    tmp_path: Path, monkeypatch
) -> None:
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

    callback_acks: list[dict[str, object]] = []

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            callback_acks.append({"json": json, "timeout": timeout})
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-missing",
        "data": "prio_set::R",
        "message": {"chat": {"id": "chat"}, "message_id": 11},
    }
    processor.handle_callback_query(callback)

    assert sent == []
    assert callback_acks == [
        {
            "json": {
                "callback_query_id": "cb-prio-missing",
                "text": _norm("Не нашёл письмо для изменения"),
                "show_alert": False,
            },
            "timeout": 5,
        }
    ]


def test_priority_callback_edits_same_message_and_changes_text(
    tmp_path: Path, monkeypatch
) -> None:
    test_priority_callback_edits_same_message_and_updates_priority_text(
        tmp_path, monkeypatch
    )


def test_priority_callback_persists_snapshot_priority(
    tmp_path: Path, monkeypatch
) -> None:
    test_priority_callback_updates_snapshot_priority(tmp_path, monkeypatch)


def test_priority_callback_survives_rerender(tmp_path: Path, monkeypatch) -> None:
    test_priority_callback_updates_snapshot_priority(tmp_path, monkeypatch)


def test_priority_callback_missing_message_id_answers_without_second_message(
    tmp_path: Path, monkeypatch
) -> None:
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

    callback_acks: list[dict[str, object]] = []

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            callback_acks.append({"json": json, "timeout": timeout})
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-no-msg-id",
        "data": f"mb:prio:{email_id}:Y",
        "message": {"chat": {"id": "chat"}},
    }
    processor.handle_callback_query(callback)

    assert sent == []
    assert callback_acks and _norm(callback_acks[0]["json"]["text"]) == _norm(
        "Не могу отредактировать"
    )


def test_no_mojibake_in_help_output(tmp_path: Path) -> None:
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

    assert sent
    for token in ("РІР‚", "Р С•РЎвЂљ", "СЂСџ"):
        assert token not in sent[-1]


def test_no_mojibake_in_stats_output(tmp_path: Path) -> None:
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

    processor.handle_message({"chat": {"id": "chat"}, "text": "/stats"})

    assert sent
    for token in ("РІР‚", "Р С•РЎвЂљ", "СЂСџ"):
        assert token not in sent[-1]


def test_status_matches_startup_health_when_llm_active(tmp_path: Path) -> None:
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

    from mailbot_v26.system_health import system_health

    system_health.reset()
    system_health.update_component("CRM", True)
    system_health.update_component("Telegram", True)
    system_health.update_component("LLM", True)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})

    assert _norm("Режим: Полный режим") in _norm(sent[-1])
    assert _norm("AI: активен") in _norm(sent[-1])


def test_status_not_degraded_when_direct_llm_is_active(tmp_path: Path) -> None:
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

    from mailbot_v26.system_health import system_health

    system_health.reset()
    system_health.update_component("CRM", True)
    system_health.update_component("Telegram", True)
    system_health.update_component("LLM", True)

    processor.handle_message({"chat": {"id": "chat"}, "text": "/status"})

    assert _norm("Деградация: без AI") not in _norm(sent[-1])


def test_priority_callback_edits_message_with_manual_priority_marker(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )
    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-marker",
        "data": f"prio_set:{email_id}:R",
        "message": {"chat": {"id": "chat"}, "message_id": 777, "text": "old"},
    }
    processor.handle_callback_query(callback)

    assert edited
    rendered = _norm(str(edited[-1]["html_text"]))
    assert "Приоритет:" in rendered
    assert "вручную" in rendered


def test_manual_priority_marker_not_duplicated_on_rerender(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )
    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-marker-1",
        "data": f"prio_set:{email_id}:Y",
        "message": {"chat": {"id": "chat"}, "message_id": 778, "text": "old"},
    }
    processor.handle_callback_query(callback)
    callback["id"] = "cb-prio-marker-2"
    processor.handle_callback_query(callback)

    assert len(edited) >= 2
    rendered = _norm(str(edited[-1]["html_text"]))
    assert rendered.count("вручную") == 1


def test_user_override_priority_survives_enrichment(
    tmp_path: Path, monkeypatch
) -> None:
    test_priority_callback_updates_snapshot_priority(tmp_path, monkeypatch)


def test_no_mojibake_in_status_output(tmp_path: Path) -> None:
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

    assert sent
    for token in ("РІР‚", "Р С•РЎвЂљ", "СЂСџ"):
        assert token not in sent[-1]


def test_manual_priority_marker_appears_after_callback(
    tmp_path: Path, monkeypatch
) -> None:
    test_priority_callback_edits_message_with_manual_priority_marker(
        tmp_path, monkeypatch
    )


def test_priority_circle_present_after_manual_override(
    tmp_path: Path, monkeypatch
) -> None:
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

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )
    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-prio-circle",
        "data": f"prio_set:{email_id}:R",
        "message": {"chat": {"id": "chat"}, "message_id": 909, "text": "old"},
    }
    processor.handle_callback_query(callback)

    assert edited
    rendered = _norm(str(edited[-1]["html_text"]))
    assert rendered.startswith("🔴")


def test_priority_callback_still_edits_same_message(
    tmp_path: Path, monkeypatch
) -> None:
    test_priority_callback_edits_same_message_and_updates_priority_text(
        tmp_path, monkeypatch
    )
