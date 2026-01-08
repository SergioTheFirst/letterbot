import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _insert_email(
    db_path: Path,
    *,
    email_id: int,
    account_email: str,
    from_email: str,
    received_at: datetime,
    action_line: str = "",
    body_summary: str = "",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, action_line, body_summary, received_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                email_id,
                account_email,
                from_email,
                action_line,
                body_summary,
                received_at.isoformat(),
            ),
        )


def _insert_event(
    db_path: Path,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    email_id: int,
    payload: dict[str, object],
) -> None:
    event = EventV1(
        event_type=event_type,
        ts_utc=ts_utc,
        account_id=account_id,
        entity_id=None,
        email_id=email_id,
        payload=payload,
        schema_version=1,
    )
    payload_json = json.dumps(payload, ensure_ascii=False)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO events_v1 (
                event_type, ts_utc, ts, account_id, entity_id, email_id, payload,
                payload_json, schema_version, fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_type.value,
                event.ts_utc,
                None,
                event.account_id,
                None,
                event.email_id,
                payload_json,
                payload_json,
                event.schema_version,
                fingerprint(event),
            ),
        )


def _insert_processing_span(
    db_path: Path,
    *,
    span_id: str,
    ts_start: float,
    total_duration_ms: int,
    account_id: str,
    email_id: int,
    stage_durations: dict[str, int],
    outcome: str = "ok",
    error_code: str = "",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO processing_spans (
                span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
                stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
                fallback_used, outcome, error_code, health_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, ?, ?, '')
            """,
            (
                span_id,
                ts_start,
                ts_start + (total_duration_ms / 1000.0),
                total_duration_ms,
                account_id,
                email_id,
                json.dumps(stage_durations, ensure_ascii=False),
                outcome,
                error_code,
            ),
        )


def _build_app(tmp_path: Path) -> tuple[Path, object]:
    db_path = tmp_path / "archive.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    return db_path, app


def test_archive_auth_required_and_headers(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    now = datetime.now(timezone.utc)
    _insert_email(
        db_path,
        email_id=1,
        account_email="acct@example.com",
        from_email="sender@example.com",
        received_at=now,
        action_line="Follow up",
        body_summary="Check status",
    )

    with app.test_client() as client:
        response = client.get("/archive")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")

        client.post("/login", data={"password": "pw"})
        page = client.get("/archive")
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert "Time (UTC)" in body
        assert "TG status" in body
        assert "E2E latency" in body


def test_archive_pagination_order_deterministic(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    now = datetime.now(timezone.utc)
    for email_id in [1, 2, 3]:
        _insert_email(
            db_path,
            email_id=email_id,
            account_email="acct@example.com",
            from_email=f"sender{email_id}@example.com",
            received_at=now,
        )
    _insert_event(
        db_path,
        event_type=EventType.TELEGRAM_DELIVERED,
        ts_utc=now.timestamp(),
        account_id="acct@example.com",
        email_id=3,
        payload={"occurred_at_utc": now.timestamp()},
    )

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get("/archive", query_string={"account_emails": "acct@example.com"})
        body = page.get_data(as_text=True)
        ids = [int(match) for match in re.findall(r'data-email-id="(\d+)"', body)]
        assert ids[:3] == [3, 2, 1]


def test_email_details_timeline_sorted(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    now = datetime.now(timezone.utc)
    _insert_email(
        db_path,
        email_id=42,
        account_email="acct@example.com",
        from_email="sender@example.com",
        received_at=now,
    )
    _insert_processing_span(
        db_path,
        span_id="span-b",
        ts_start=1000.0,
        total_duration_ms=120,
        account_id="acct@example.com",
        email_id=42,
        stage_durations={"parse": 50, "send": 30},
    )
    _insert_processing_span(
        db_path,
        span_id="span-a",
        ts_start=1000.0,
        total_duration_ms=80,
        account_id="acct@example.com",
        email_id=42,
        stage_durations={"analyze": 40},
    )

    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get("/email/42")
        body = page.get_data(as_text=True)
        stages = re.findall(r'data-stage="([^"]+)"', body)
        assert stages[:3] == ["analyze", "parse", "send"]
