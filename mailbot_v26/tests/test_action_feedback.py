from __future__ import annotations

import json
import sqlite3

from mailbot_v26 import config_loader
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import record_action_feedback, record_priority_correction
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode


def _write_accounts(tmp_path, content: str) -> None:
    (tmp_path / "accounts.ini").write_text(content, encoding="utf-8")


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


def test_feedback_emits_event_v1(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "priority_feedback_events.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _write_accounts(
        tmp_path,
        """[primary]
login = account@example.com
password = secret
telegram_chat_id = chat

[alt]
login = alt@example.com
password = secret
telegram_chat_id = chat
""",
    )
    monkeypatch.setattr(config_loader, "CONFIG_DIR", tmp_path)
    config_loader._load_account_scopes.cache_clear()

    first_id = record_priority_correction(
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
        source="preview_buttons",
    )
    second_id = record_priority_correction(
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
        source="preview_buttons",
    )

    with sqlite3.connect(db_path) as conn:
        events = conn.execute(
            "SELECT event_type, email_id, payload FROM events_v1"
        ).fetchall()
        feedback_rows = conn.execute(
            "SELECT id FROM priority_feedback",
        ).fetchall()

    assert first_id == second_id
    assert len(feedback_rows) == 1
    assert len(events) == 1
    assert events[0][0] == "priority_correction_recorded"
    payload = json.loads(events[0][2])
    assert payload["new_priority"] == "🔴"
    assert payload["old_priority"] == "🟡"
    assert payload["engine"] == "priority_v2_shadow"
    assert payload["source"] == "preview_buttons"
    assert payload["chat_scope"] == "tg:chat"
    assert payload["account_emails"] == ["account@example.com", "alt@example.com"]


def test_get_account_scope() -> None:
    scope = config_loader.get_account_scope(
        chat_id="chat",
        account_email="account@example.com",
        account_emails=["alt@example.com", "account@example.com", ""],
    )
    assert scope == {
        "chat_scope": "tg:chat",
        "account_email": "account@example.com",
        "account_emails": ["account@example.com", "alt@example.com"],
    }
    assert config_loader.get_account_scope(chat_id="") == {}


def test_surprise_event_emitted_when_enabled(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "priority_feedback_surprise.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _write_accounts(
        tmp_path,
        """[primary]
login = account@example.com
password = secret
telegram_chat_id = chat

[alt]
login = alt@example.com
password = secret
telegram_chat_id = chat
""",
    )
    monkeypatch.setattr(config_loader, "CONFIG_DIR", tmp_path)
    config_loader._load_account_scopes.cache_clear()

    record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=202,
        correction="🔴",
        old_priority="🔵",
        entity_id="entity-9",
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
        contract_event_emitter=contract_emitter,
        engine="priority_v2_shadow",
        source="preview_buttons",
        surprise_mode="shadow",
    )

    with sqlite3.connect(db_path) as conn:
        event_rows = conn.execute(
            "SELECT event_type, payload FROM events_v1"
        ).fetchall()
    event_types = [row[0] for row in event_rows]

    assert "priority_correction_recorded" in event_types
    assert "surprise_detected" in event_types
    surprise_payload = next(
        json.loads(row[1]) for row in event_rows if row[0] == "surprise_detected"
    )
    assert surprise_payload["chat_scope"] == "tg:chat"
    assert surprise_payload["account_emails"] == ["account@example.com", "alt@example.com"]


def test_priority_correction_payload_without_scope(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "priority_feedback_noscope.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    _write_accounts(
        tmp_path,
        """[primary]
login = account@example.com
password = secret
""",
    )
    monkeypatch.setattr(config_loader, "CONFIG_DIR", tmp_path)
    config_loader._load_account_scopes.cache_clear()

    record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=303,
        correction="🔴",
        old_priority="🟡",
        entity_id="entity-10",
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
        contract_event_emitter=contract_emitter,
        engine="priority_v2_shadow",
        source="preview_buttons",
    )

    with sqlite3.connect(db_path) as conn:
        event_rows = conn.execute(
            "SELECT event_type, payload FROM events_v1"
        ).fetchall()
    payload = json.loads(event_rows[0][1])
    assert "chat_scope" not in payload
    assert "account_emails" not in payload
