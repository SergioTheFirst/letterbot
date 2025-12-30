from __future__ import annotations

import json
import sqlite3

from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import record_action_feedback, record_priority_correction
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode


def test_feedback_persisted_and_logged(tmp_path, caplog) -> None:
    db_path = tmp_path / "feedback.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    proposed_action = {"type": "FOLLOW_UP", "text": "Follow-up через 2 дня", "confidence": 0.88}

    caplog.set_level("INFO")
    feedback_id = record_action_feedback(
        knowledge_db=knowledge_db,
        email_id="email-123",
        proposed_action=proposed_action,
        decision="accepted",
        user_note="ok",
        system_mode=OperationalMode.FULL,
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT id, email_id, proposed_action, decision, user_note FROM action_feedback"
        ).fetchone()

    assert row is not None
    assert row[0] == feedback_id
    assert row[1] == "email-123"
    assert row[3] == "accepted"
    assert row[4] == "ok"
    stored_payload = json.loads(row[2])
    assert stored_payload["type"] == "FOLLOW_UP"

    assert any(
        json.loads(record.message).get("event") == "preview_accepted"
        for record in caplog.records
        if record.message.startswith("{")
    )


def test_priority_correction_persisted(tmp_path, caplog) -> None:
    db_path = tmp_path / "priority_feedback.sqlite"
    knowledge_db = KnowledgeDB(db_path)

    caplog.set_level("INFO")
    feedback_id = record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=42,
        correction="high",
        entity_id="entity-7",
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, email_id, kind, value, entity_id, sender_email, account_email
            FROM priority_feedback
            """
        ).fetchone()

    assert row is not None
    assert row[0] == feedback_id
    assert row[1] == "42"
    assert row[2] == "priority_correction"
    assert row[3] == "high"
    assert row[4] == "entity-7"
    assert row[5] == "sender@example.com"
    assert row[6] == "account@example.com"


def test_feedback_emits_event_v1(tmp_path) -> None:
    db_path = tmp_path / "priority_feedback_events.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    contract_emitter = ContractEventEmitter(db_path)

    record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=101,
        correction="🔴",
        old_priority="🟡",
        entity_id="entity-7",
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
        contract_event_emitter=contract_emitter,
        engine="priority_v2_shadow",
        model_version="v2.0",
        reason_codes=["rule_a"],
    )
    record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=101,
        correction="🔴",
        old_priority="🟡",
        entity_id="entity-7",
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
        contract_event_emitter=contract_emitter,
        engine="priority_v2_shadow",
        model_version="v2.0",
        reason_codes=["rule_a"],
    )

    with sqlite3.connect(db_path) as conn:
        events = conn.execute(
            "SELECT event_type, email_id, payload FROM events_v1"
        ).fetchall()
        feedback_count = conn.execute(
            "SELECT COUNT(*) FROM priority_feedback"
        ).fetchone()[0]

    assert feedback_count == 2
    assert len(events) == 1
    assert events[0][0] == "priority_correction_recorded"
    payload = json.loads(events[0][2])
    assert payload["new_priority"] == "🔴"
    assert payload["old_priority"] == "🟡"
    assert payload["engine"] == "priority_v2_shadow"
