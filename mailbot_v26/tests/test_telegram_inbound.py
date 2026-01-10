from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.config.auto_priority_gate import AutoPriorityGateConfig
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.features.flags import FeatureFlags
from mailbot_v26.insights.auto_priority_quality_gate import GateResult
from mailbot_v26.llm.runtime_flags import RuntimeFlagStore
from mailbot_v26.observability.event_emitter import EventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.storage.runtime_overrides import RuntimeOverrideStore
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
