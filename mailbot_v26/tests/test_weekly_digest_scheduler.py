from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.pipeline import weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def test_weekly_calibration_proposals_event_emitted(tmp_path) -> None:
    db_path = tmp_path / "digest.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    analytics = KnowledgeAnalytics(db_path)
    now = datetime.now(timezone.utc)

    for offset in range(3):
        emitter.emit(
            EventV1(
                event_type=EventType.PRIORITY_CORRECTION_RECORDED,
                ts_utc=now.timestamp() + offset,
                account_id="acc@example.com",
                entity_id="entity-a",
                email_id=None,
                payload={"old_priority": "🔴", "new_priority": "🟡"},
            )
        )

    weekly_digest._collect_weekly_data(
        analytics=analytics,
        account_email="acc@example.com",
        account_emails=["acc@example.com"],
        week_key="2025-W01",
        include_weekly_calibration_report=True,
        weekly_calibration_window_days=7,
        weekly_calibration_top_n=3,
        weekly_calibration_min_corrections=1,
        contract_event_emitter=emitter,
        now=now,
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT payload_json FROM events_v1 WHERE event_type = ?",
            (EventType.CALIBRATION_PROPOSALS_GENERATED.value,),
        ).fetchone()

    assert row is not None
    payload = json.loads(row[0])
    assert payload["week_key"] == "2025-W01"
    assert payload["proposals_count"] == 1
    assert payload["top_labels"] == ["entity-a"]
    assert "transition" not in payload
    assert "hint" not in payload
