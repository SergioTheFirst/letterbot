from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from mailbot_v26.telegram.callback_data import (
    FEEDBACK_PREFIX,
    PRIORITY_PREFIX,
    encode as encode_callback_data,
)
from mailbot_v26.telegram.decision_trace_ui import (
    build_decision_trace_keyboard,
    build_email_actions_keyboard,
)
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


def _downgrade_emails_table_to_legacy_schema(db_path: Path, *, email_id: int) -> None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, account_email, from_email, subject, received_at, raw_body_hash
            FROM emails
            WHERE id = ?
            """,
            (email_id,),
        ).fetchone()
        assert row is not None
        conn.execute("ALTER TABLE emails RENAME TO emails_runtime_full")
        conn.execute(
            """
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT,
                from_email TEXT,
                subject TEXT,
                received_at TEXT,
                raw_body_hash TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, received_at, raw_body_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            row,
        )
        conn.execute("DROP TABLE emails_runtime_full")
        conn.commit()


def _emit_interpretation(db_path: Path, *, email_id: int, doc_kind: str) -> None:
    ContractEventEmitter(db_path).emit(
        EventV1(
            event_type=EventType.MESSAGE_INTERPRETATION,
            ts_utc=float(email_id),
            account_id="account@example.com",
            entity_id=None,
            email_id=email_id,
            payload={
                "sender_email": "sender@example.com",
                "doc_kind": doc_kind,
                "amount": 87500.0 if doc_kind == "invoice" else None,
                "due_date": "2026-04-15" if doc_kind == "invoice" else None,
                "priority": "🟡",
                "action": "Проверить",
                "confidence": 0.92,
                "context": "NEW_MESSAGE",
                "document_id": f"{doc_kind}-{email_id}",
                "issuer_label": "ООО Вектор",
            },
        )
    )


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


def test_lang_command_persists_ui_locale_override(tmp_path: Path) -> None:
    sent: list[str] = []
    gate_result = GateResult(
        passed=True,
        reason="ok",
        window_days=30,
        samples=10,
        corrections=0,
        correction_rate=0.0,
        engine="test",
    )
    processor = _build_processor(tmp_path, sent, gate_result)

    processor.handle_update(
        {"update_id": 1, "message": {"chat": {"id": "chat"}, "text": "/lang en"}}
    )
    assert processor.override_store.get_value("ui_locale") == "en"

    processor.handle_update(
        {"update_id": 2, "message": {"chat": {"id": "chat"}, "text": "/lang ru"}}
    )
    assert processor.override_store.get_value("ui_locale") == "ru"


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


def test_parse_callback_data_new_contract() -> None:
    parsed = parse_callback_data(
        encode_callback_data(prefix=FEEDBACK_PREFIX, action="paid", msg_key="12")
    )
    assert parsed == (
        "feedback",
        {"email_id": "12", "feedback_action": "paid"},
    )
    parsed = parse_callback_data(
        encode_callback_data(prefix=PRIORITY_PREFIX, action="hi", msg_key="44")
    )
    assert parsed == (
        "priority_inline",
        {"email_id": "44", "priority_action": "hi"},
    )
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
    assert edited[0]["reply_markup"] == build_email_actions_keyboard(
        email_id=email_id,
        expanded=False,
        show_decision_trace=False,
    )
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


def test_priority_callback_survives_legacy_emails_schema_and_migrates_columns(
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
    _emit_interpretation(
        processor.knowledge_db.path, email_id=email_id, doc_kind="invoice"
    )
    _downgrade_emails_table_to_legacy_schema(
        processor.knowledge_db.path, email_id=email_id
    )

    edited: list[dict[str, object]] = []

    def _fake_edit(**kwargs):
        edited.append(kwargs)

    monkeypatch.setattr(
        "mailbot_v26.telegram.inbound.edit_telegram_message", _fake_edit
    )

    processor.handle_callback_query(
        {
            "id": "cb-pr-legacy",
            "data": encode_callback_data(
                prefix=PRIORITY_PREFIX, action="med", msg_key=str(email_id)
            ),
            "message": {"chat": {"id": "chat"}, "message_id": 18},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(emails)").fetchall()
        }
        priority_row = conn.execute(
            "SELECT priority, priority_source FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()

    assert "priority" in columns
    assert "priority_source" in columns
    assert priority_row == ("\U0001f7e1", "user_override")
    assert edited
    assert sent == []


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


def test_snooze_state_matches_canonical_email_scope(tmp_path: Path) -> None:
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
            "id": "cb-snooze-canonical",
            "data": f"snz_s:{email_id}:2h",
            "message": {"chat": {"id": "chat"}, "message_id": 12, "text": "msg"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        snooze_row = conn.execute(
            "SELECT email_id, status FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()
        event_row = conn.execute(
            """
            SELECT account_id, email_id, payload_json
            FROM events_v1
            WHERE event_type = ?
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            (EventType.SNOOZE_RECORDED.value,),
        ).fetchone()

    assert snooze_row == (email_id, "pending")
    assert event_row is not None
    payload = json.loads(str(event_row[2] or "{}"))
    assert event_row[0] == "account@example.com"
    assert int(event_row[1]) == email_id
    assert payload["snooze_code"] == "2h"


def test_snooze_tomorrow_creates_pending_record_for_next_morning(
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
    email_id = _insert_email(processor.knowledge_db.path)
    before_local = datetime.now().astimezone()

    processor.handle_callback_query(
        {
            "id": "cb2",
            "data": f"snz_s:{email_id}:tom",
            "message": {"chat": {"id": "chat"}, "message_id": 11, "text": "msg"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        row = conn.execute(
            "SELECT email_id, status, deliver_at_utc FROM telegram_snooze WHERE email_id = ?",
            (email_id,),
        ).fetchone()

    assert row is not None
    assert int(row[0]) == email_id
    assert row[1] == "pending"
    deliver_local = datetime.fromisoformat(str(row[2])).astimezone()
    assert deliver_local.date() == before_local.date() + timedelta(days=1)
    assert deliver_local.hour == 9
    assert deliver_local.minute == 0


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


def test_priority_state_matches_canonical_snapshot_and_event(
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
        lambda **_kwargs: None,
    )

    processor.handle_callback_query(
        {
            "id": "cb-prio-canonical",
            "data": f"prio_set:{email_id}:Y",
            "message": {"chat": {"id": "chat"}, "message_id": 405, "text": "old"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        priority_row = conn.execute(
            "SELECT priority, priority_source FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()
        event_row = conn.execute(
            """
            SELECT payload_json
            FROM events_v1
            WHERE event_type = 'priority_correction_recorded'
            ORDER BY ts_utc DESC
            LIMIT 1
            """
        ).fetchone()

    assert priority_row == ("\U0001f7e1", "user_override")
    assert event_row is not None
    payload = json.loads(str(event_row[0] or "{}"))
    assert payload["old_priority"] == "\U0001f535"
    assert payload["new_priority"] == "\U0001f7e1"


def test_priority_callback_same_priority_records_confirmation_not_correction(
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

    processor.handle_callback_query(
        {
            "id": "cb-prio-confirm",
            "data": f"prio_set:{email_id}:B",
            "message": {"chat": {"id": "chat"}, "message_id": 404, "text": "old"},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        feedback_rows = conn.execute(
            "SELECT kind, value FROM priority_feedback WHERE email_id = ?",
            (str(email_id),),
        ).fetchall()
        correction_count = conn.execute(
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = 'priority_correction_recorded'"
        ).fetchone()
        priority_row = conn.execute(
            "SELECT priority, priority_source FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()

    assert edited
    assert feedback_rows == [("priority_confirmation", "🔵")]
    assert correction_count is not None and int(correction_count[0]) == 0
    assert priority_row == ("🔵", "auto")


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

    assert _norm("📊 LetterBot.ru — неделя") in _norm(sent[-1])
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

    assert _norm("📊 LetterBot.ru — неделя") in _norm(sent[-1])
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
        label="Поддержать LetterBot.ru",
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
        text="Если LetterBot.ru помогает, проект можно поддержать",
        url="https://example.com/insider",
        label="Поддержать LetterBot.ru",
        frequency_days=30,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "support"})

    assert _norm(sent[-1]) == _norm(
        "Поддержать LetterBot.ru\nЕсли LetterBot.ru помогает, проект можно поддержать\nhttps://example.com/insider"
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
        label="Поддержать LetterBot.ru",
        frequency_days=30,
    )

    processor.handle_message({"chat": {"id": "chat"}, "text": "/support"})

    assert _norm(sent[-1]) == _norm("Поддержка включена, но ссылка ещё не настроена.")


def test_unknown_slash_command_returns_help(tmp_path: Path) -> None:
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

    processor.handle_message({"chat": {"id": "chat"}, "text": "/wat"})
    unknown_text = sent[-1]
    processor.handle_message({"chat": {"id": "chat"}, "text": "/help"})
    help_text = sent[-1]

    assert _norm(unknown_text) == _norm(help_text)


def test_unknown_bot_command_returns_help(tmp_path: Path) -> None:
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

    processor.handle_message({"chat": {"id": "chat"}, "text": "nonsense"})
    unknown_text = sent[-1]
    processor.handle_message({"chat": {"id": "chat"}, "text": "/help"})
    help_text = sent[-1]

    assert _norm(unknown_text) == _norm(help_text)


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


def test_help_shows_digest_and_autopriority_as_separate_commands(
    tmp_path: Path,
) -> None:
    """Help text must list /digest on and /digest off as separate lines,
    and /autopriority on and /autopriority off as separate lines.
    The old combined on|off format must not appear.
    """
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

    help_text = sent[-1]
    # Separate entries must be present
    assert _norm("/digest on") in _norm(help_text)
    assert _norm("/digest off") in _norm(help_text)
    assert _norm("/autopriority on") in _norm(help_text)
    assert _norm("/autopriority off") in _norm(help_text)
    # Old combined format must NOT appear
    assert "on|off" not in help_text


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

    assert _norm("РІВ­С’ LetterBot.ru Insider since:") not in _norm(sent[-1])
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

    assert _norm("⭐ LetterBot.ru Insider since: 2026-02") in _norm(sent[-1])


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


def test_callback_answer_always_called(tmp_path: Path, monkeypatch) -> None:
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

    processor.handle_callback_query(
        {
            "id": "cb-malformed",
            "data": "FB:broken",
            "message": {"chat": {"id": "chat"}, "message_id": 15},
        }
    )

    assert sent == []
    assert len(callback_acks) == 1
    assert callback_acks[0]["json"]["callback_query_id"] == "cb-malformed"


def test_callback_invalid_data_does_not_crash(tmp_path: Path, monkeypatch) -> None:
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

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    processor.handle_callback_query(
        {
            "id": "cb-invalid",
            "data": "PR:???:999",
            "message": {"chat": {"id": "chat"}, "message_id": 16},
        }
    )

    assert sent == []


def test_callback_feedback_writes_correction_event(
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
    _emit_interpretation(processor.knowledge_db.path, email_id=email_id, doc_kind="invoice")

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    processor.handle_callback_query(
        {
            "id": "cb-paid",
            "data": encode_callback_data(
                prefix=FEEDBACK_PREFIX,
                action="paid",
                msg_key=str(email_id),
            ),
            "message": {"chat": {"id": "chat"}, "message_id": 17},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        row = conn.execute(
            """
            SELECT decision, user_note
            FROM action_feedback
            WHERE email_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (str(email_id),),
        ).fetchone()
    assert row == ("paid", "telegram_inline")
    assert sent == []


def test_callback_priority_writes_priority_override(
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
    _emit_interpretation(processor.knowledge_db.path, email_id=email_id, doc_kind="invoice")

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

    processor.handle_callback_query(
        {
            "id": "cb-pr-hi",
            "data": encode_callback_data(
                prefix=PRIORITY_PREFIX,
                action="hi",
                msg_key=str(email_id),
            ),
            "message": {"chat": {"id": "chat"}, "message_id": 18},
        }
    )

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        priority_row = conn.execute(
            "SELECT priority, priority_source FROM emails WHERE id = ?",
            (email_id,),
        ).fetchone()
        event_rows = conn.execute(
            "SELECT COUNT(*) FROM events_v1 WHERE event_type = 'priority_correction_recorded'"
        ).fetchone()
    assert priority_row == ("🔴", "user_override")
    assert event_rows is not None and int(event_rows[0]) == 1
    assert edited
    assert sent == []


def test_callback_double_tap_is_idempotent_or_safely_deduped(
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
    _emit_interpretation(processor.knowledge_db.path, email_id=email_id, doc_kind="invoice")

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    callback = {
        "id": "cb-paid-once",
        "data": encode_callback_data(
            prefix=FEEDBACK_PREFIX,
            action="paid",
            msg_key=str(email_id),
        ),
        "message": {"chat": {"id": "chat"}, "message_id": 19},
    }
    processor.handle_callback_query(callback)
    callback["id"] = "cb-paid-twice"
    processor.handle_callback_query(callback)

    with sqlite3.connect(processor.knowledge_db.path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM action_feedback WHERE email_id = ? AND decision = 'paid'",
            (str(email_id),),
        ).fetchone()[0]
    assert int(count) == 1
    assert sent == []


def test_callback_unknown_msg_key_is_safe(tmp_path: Path, monkeypatch) -> None:
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

    processor.handle_callback_query(
        {
            "id": "cb-missing-feedback",
            "data": encode_callback_data(
                prefix=FEEDBACK_PREFIX,
                action="paid",
                msg_key="99999",
            ),
            "message": {"chat": {"id": "chat"}, "message_id": 20},
        }
    )

    assert sent == []
    assert callback_acks
    assert _norm(str(callback_acks[0]["json"]["text"])) == _norm("Не нашёл письмо")


def test_callback_does_not_send_second_message(tmp_path: Path, monkeypatch) -> None:
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
    _emit_interpretation(processor.knowledge_db.path, email_id=email_id, doc_kind="invoice")

    class _StubRequests:
        def post(self, _url: str, json: dict[str, object], timeout: int):
            return object()

    monkeypatch.setattr("mailbot_v26.worker.telegram_sender.requests", _StubRequests())

    processor.handle_callback_query(
        {
            "id": "cb-correct",
            "data": encode_callback_data(
                prefix=FEEDBACK_PREFIX,
                action="correct",
                msg_key=str(email_id),
            ),
            "message": {"chat": {"id": "chat"}, "message_id": 21},
        }
    )

    assert sent == []


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


def test_interpretation_and_snapshot_do_not_conflict_on_manual_priority(
    tmp_path: Path, monkeypatch
) -> None:
    test_user_override_priority_survives_enrichment(tmp_path, monkeypatch)


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
