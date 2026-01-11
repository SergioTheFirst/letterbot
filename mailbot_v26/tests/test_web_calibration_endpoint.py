from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.decision_trace_v1 import (
    DecisionTraceV1,
    to_canonical_json,
)
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def test_calibration_endpoint_pii_free(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "calibration_api.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    ts_utc = datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp()
    trace = DecisionTraceV1(
        decision_key="abc123",
        decision_kind="PRIORITY_HEURISTIC",
        anchor_ts_utc=ts_utc,
        signals_evaluated=["A", "B"],
        signals_fired=["A"],
        evidence={"matched": 1, "total": 2},
        model_fingerprint="model-fp",
        explain_codes=["CODE_A"],
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DECISION_TRACE_RECORDED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=None,
            email_id=1,
            payload={},
            payload_json=to_canonical_json(trace),
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.PRIORITY_CORRECTION_RECORDED,
            ts_utc=ts_utc,
            account_id="account@example.com",
            entity_id=None,
            email_id=1,
            payload={"decision_key": "abc123", "model_fingerprint": "model-fp"},
        )
    )

    monkeypatch.setenv("WEB_OBSERVABILITY_TOKEN", "secret-token")
    app = create_app(
        db_path=db_path,
        password="pass",
        secret_key="secret",
    )
    client = app.test_client()
    resp = client.get(
        "/api/v1/cockpit/calibration?days=30&max_rows=1000&token=secret-token",
        headers={"X-Api-Token": "secret-token"},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert "models" in payload
    raw = json.dumps(payload)
    assert "@" not in raw
