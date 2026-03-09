from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.feedback import record_priority_correction
from mailbot_v26.observability.decision_trace_v1 import (
    DecisionTraceV1,
    compute_decision_key,
    to_canonical_json,
)
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.system_health import OperationalMode


def _seed_trace(db_path: Path, *, email_id: int, ts_utc: float) -> None:
    emitter = ContractEventEmitter(db_path)
    trace = DecisionTraceV1(
        decision_key=compute_decision_key(
            account_id="account@example.com",
            email_id=email_id,
            decision_kind="PRIORITY_HEURISTIC",
            anchor_ts_utc=ts_utc,
        ),
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=ts_utc,
        signals_evaluated=["SIGNAL_A", "SIGNAL_B"],
        signals_fired=["SIGNAL_A"],
        evidence={"matched": 1, "total": 2},
        model_fingerprint="model-fp",
        explain_codes=["CODE_A"],
    )
    event = EventV1(
        event_type=EventType.DECISION_TRACE_RECORDED,
        ts_utc=ts_utc,
        account_id="account@example.com",
        entity_id=None,
        email_id=email_id,
        payload={},
        payload_json=to_canonical_json(trace),
    )
    emitter.emit(event)


def test_priority_correction_event_enriched_without_pii(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_correction.sqlite"
    knowledge_db = KnowledgeDB(db_path)
    contract_emitter = ContractEventEmitter(db_path)
    now_ts = datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp()
    _seed_trace(db_path, email_id=55, ts_utc=now_ts)

    record_priority_correction(
        knowledge_db=knowledge_db,
        email_id=55,
        correction="🔴",
        entity_id=None,
        sender_email="sender@example.com",
        account_email="account@example.com",
        system_mode=OperationalMode.FULL,
        contract_event_emitter=contract_emitter,
        engine="priority_v2_shadow",
        source="telegram_inbound",
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT payload
            FROM events_v1
            WHERE event_type = ?
            """,
            (EventType.PRIORITY_CORRECTION_RECORDED.value,),
        ).fetchone()

    payload = json.loads(row[0])
    assert "sender_email" not in payload
    assert "account_email" not in payload
    assert "account_emails" not in payload
    assert payload["decision_key"]
    assert payload["model_fingerprint"] == "model-fp"
    assert payload["evidence"] == {"matched": 1, "total": 2}
    assert payload["signals_evaluated_count"] == 2
    assert payload["signals_fired_count"] == 1
    assert payload["original_decision"] == ""
    assert payload["corrected_decision"] == payload["new_priority"]
    assert payload["confidence"] == 1.0
    assert payload["issuer_fingerprint"].startswith("issuer:")
    assert payload["issuer_identity_confidence"] == "medium"
    timestamp_iso = payload["timestamp_iso"]
    parsed_ts = datetime.fromisoformat(timestamp_iso)
    assert parsed_ts.tzinfo is not None
