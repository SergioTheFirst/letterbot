from __future__ import annotations

import json
import sqlite3

from mailbot_v26.feedback import record_action_feedback
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
