from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.observability.decision_trace_v1 import (
    get_default_decision_trace_emitter,
)
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def test_decision_trace_health_endpoint(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "health.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    now_ts = datetime(2026, 1, 20, tzinfo=timezone.utc).timestamp()
    for email_id in (1, 2, 3):
        emitter.emit(
            EventV1(
                event_type=EventType.TELEGRAM_DELIVERED,
                ts_utc=now_ts + email_id,
                account_id="account@example.com",
                entity_id=None,
                email_id=email_id,
                payload={},
            )
        )
    emitter.emit(
        EventV1(
            event_type=EventType.DECISION_TRACE_RECORDED,
            ts_utc=now_ts + 1,
            account_id="account@example.com",
            entity_id=None,
            email_id=1,
            payload={},
            payload_json="{}",
        )
    )
    emitter.emit(
        EventV1(
            event_type=EventType.DECISION_TRACE_RECORDED,
            ts_utc=now_ts + 2,
            account_id="account@example.com",
            entity_id=None,
            email_id=2,
            payload={},
            payload_json="{}",
        )
    )

    default_emitter = get_default_decision_trace_emitter()
    with default_emitter._lock:
        default_emitter.attempted = 0
        default_emitter.succeeded = 0
        default_emitter.dropped = 0
        default_emitter.disabled = False
        default_emitter.last_drop_reason = None
        default_emitter.breaker_until_ts = None
        default_emitter.failure_log_path = tmp_path / "decision_trace_failures.ndjson"

    class FailingEmitter:
        def __init__(self) -> None:
            self.db_path = db_path

        def emit(self, event: EventV1) -> bool:
            raise RuntimeError("boom")

    failer = FailingEmitter()
    default_emitter.emit(
        failer,
        EventV1(
            event_type=EventType.DECISION_TRACE_RECORDED,
            ts_utc=now_ts,
            account_id="account@example.com",
            entity_id=None,
            email_id=9,
            payload={},
            payload_json="{}",
        ),
    )

    monkeypatch.setenv("WEB_OBSERVABILITY_TOKEN", "secret-token")
    app = create_app(
        db_path=db_path,
        password="pass",
        secret_key="secret",
    )
    client = app.test_client()
    resp = client.get(
        "/api/v1/cockpit/decision-trace/health?token=secret-token",
        headers={"X-Api-Token": "secret-token"},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert "snapshot" in payload
    assert "trace_coverage" in payload
    assert "drop_log_tail" in payload
    raw = json.dumps(payload)
    assert "@" not in raw
