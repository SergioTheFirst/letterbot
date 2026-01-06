import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _insert_processing_span(
    db_path: Path,
    *,
    span_id: str,
    account_id: str,
    ts_start_utc: float,
    total_duration_ms: int,
    llm_latency_ms: int | None,
    llm_quality_score: float | None,
    fallback_used: bool,
    outcome: str,
    error_code: str = "",
    stage_durations: dict[str, int] | None = None,
) -> None:
    ts_end_utc = ts_start_utc + (total_duration_ms / 1000)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO processing_spans (
                span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
                stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
                fallback_used, outcome, error_code, health_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?, ?, '')
            """,
            (
                span_id,
                ts_start_utc,
                ts_end_utc,
                total_duration_ms,
                account_id,
                json.dumps(stage_durations or {}, ensure_ascii=False),
                llm_latency_ms,
                llm_quality_score,
                1 if fallback_used else 0,
                outcome,
                error_code,
            ),
        )


def _insert_email(
    conn: sqlite3.Connection,
    *,
    email_id: int,
    account_email: str,
    from_email: str,
    action_line: str,
    body_summary: str,
    received_at: datetime,
) -> None:
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
    conn: sqlite3.Connection,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    email_id: int,
    payload: dict[str, object] | None = None,
) -> None:
    event = EventV1(
        event_type=event_type,
        ts_utc=ts_utc,
        account_id=account_id,
        entity_id=None,
        email_id=email_id,
        payload=payload or {},
        schema_version=1,
    )
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_type.value,
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            None,
            email_id,
            json.dumps(event.payload, ensure_ascii=False),
            json.dumps(event.payload, ensure_ascii=False),
            event.schema_version,
            fingerprint(event),
        ),
    )


def _build_app_with_data(tmp_path: Path):
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    base_ts = datetime.now(timezone.utc) - timedelta(days=1)
    _insert_processing_span(
        db_path,
        span_id="s1",
        account_id="primary@example.com",
        ts_start_utc=base_ts.timestamp(),
        total_duration_ms=120,
        llm_latency_ms=50,
        llm_quality_score=0.9,
        fallback_used=False,
        outcome="ok",
        stage_durations={"parse": 30},
    )
    _insert_processing_span(
        db_path,
        span_id="s2",
        account_id="primary@example.com",
        ts_start_utc=base_ts.timestamp() + 1,
        total_duration_ms=400,
        llm_latency_ms=200,
        llm_quality_score=0.6,
        fallback_used=True,
        outcome="error",
        error_code="timeout",
        stage_durations={"llm": 180},
    )
    _insert_processing_span(
        db_path,
        span_id="s3",
        account_id="secondary@example.com",
        ts_start_utc=base_ts.timestamp() + 2,
        total_duration_ms=800,
        llm_latency_ms=300,
        llm_quality_score=0.4,
        fallback_used=False,
        outcome="ok",
        stage_durations={"final": 50},
    )
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    return app


def _build_app_with_activity(tmp_path: Path):
    db_path = tmp_path / "activity.sqlite"
    KnowledgeDB(db_path)
    base_dt = datetime.now(timezone.utc) - timedelta(hours=1)
    with sqlite3.connect(db_path) as conn:
        _insert_email(
            conn,
            email_id=1,
            account_email="primary@example.com",
            from_email="alice@example.com",
            action_line="Action needed",
            body_summary="Quarterly numbers",
            received_at=base_dt,
        )
        _insert_email(
            conn,
            email_id=2,
            account_email="primary@example.com",
            from_email="bob@example.net",
            action_line="Follow up",
            body_summary="Review contract",
            received_at=base_dt + timedelta(minutes=2),
        )
        _insert_event(
            conn,
            event_type=EventType.DELIVERY_POLICY_APPLIED,
            ts_utc=base_dt.timestamp(),
            account_id="primary@example.com",
            email_id=1,
            payload={"mode": "IMMEDIATE"},
        )
        _insert_event(
            conn,
            event_type=EventType.DELIVERY_POLICY_APPLIED,
            ts_utc=base_dt.timestamp() + 10,
            account_id="primary@example.com",
            email_id=2,
            payload={"mode": "IMMEDIATE"},
        )
        _insert_event(
            conn,
            event_type=EventType.TELEGRAM_DELIVERED,
            ts_utc=base_dt.timestamp() + 120,
            account_id="primary@example.com",
            email_id=1,
            payload={"delivered": True, "occurred_at_utc": base_dt.timestamp() + 120},
        )
        _insert_event(
            conn,
            event_type=EventType.TELEGRAM_FAILED,
            ts_utc=base_dt.timestamp() + 60,
            account_id="primary@example.com",
            email_id=2,
            payload={"delivered": False, "occurred_at_utc": base_dt.timestamp() + 60},
        )
        conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_latency_auth_required(tmp_path: Path) -> None:
    app = _build_app_with_data(tmp_path)
    with app.test_client() as client:
        response = client.get("/latency")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")


def test_latency_summary_endpoint(tmp_path: Path) -> None:
    app = _build_app_with_data(tmp_path)
    with app.test_client() as client:
        login_resp = client.post("/login", data={"password": "pw"})
        assert login_resp.status_code in (302, 303)
        response = client.get(
            "/api/v1/observability/latency_summary",
            query_string={
                "account_email": "primary@example.com",
                "account_emails": "primary@example.com,secondary@example.com",
                "window_days": "7",
            },
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert payload is not None
        assert set(["window_days", "summary", "recent_errors", "slowest", "account_email", "account_emails"]) <= set(payload.keys())


def test_latency_summary_forbidden_strings(tmp_path: Path) -> None:
    app = _build_app_with_data(tmp_path)
    forbidden = [
        "raw_body",
        "body_text",
        "subject",
        "html_text",
        "telegram_text",
        "rendered_message",
        "digest_text",
    ]
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        response = client.get(
            "/api/v1/observability/latency_summary",
            query_string={
                "account_email": "primary@example.com",
                "account_emails": "primary@example.com",
            },
        )
        text = response.get_data(as_text=True)
        for item in forbidden:
            assert item not in text


def test_latency_summary_deterministic(tmp_path: Path) -> None:
    app = _build_app_with_data(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        query = {
            "account_email": "primary@example.com",
            "account_emails": "primary@example.com,secondary@example.com",
            "window_days": "7",
        }
        first = client.get("/api/v1/observability/latency_summary", query_string=query).get_json()
        second = client.get("/api/v1/observability/latency_summary", query_string=query).get_json()
        assert first == second


def test_latency_page_masks_forbidden_phrases(tmp_path: Path) -> None:
    app = _build_app_with_activity(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get("/latency", query_string={"account_email": "primary@example.com"})
        assert page.status_code == 200
        text = page.get_data(as_text=True).lower()
        for token in ["no data", "nothing to show", "all quiet", "нет данных"]:
            assert token not in text


def test_latency_activity_table_ordering(tmp_path: Path) -> None:
    app = _build_app_with_activity(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get(
            "/latency",
            query_string={"account_email": "primary@example.com", "window_days": "7"},
        )
        assert page.status_code == 200
        body = page.get_data(as_text=True)
        assert "Recent Mail Activity" in body
        assert "Delivered (UTC)" in body and "Telegram preview" in body
        analytics = KnowledgeAnalytics(app.config["DB_PATH"], read_only=True)
        rows = analytics.recent_mail_activity(
            account_email="primary@example.com",
            account_emails=["primary@example.com"],
            window_days=7,
            limit=10,
            reveal_pii=False,
        )
        assert rows
        assert rows[0]["status"] == "Delivered"
        assert rows[0]["from_label"] == "a…@example.com"
        assert rows[1]["status"] == "Failed"


def test_latency_activity_masks_pii_by_default(tmp_path: Path) -> None:
    app = _build_app_with_activity(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get("/latency", query_string={"account_email": "primary@example.com"})
        assert page.status_code == 200
        analytics = KnowledgeAnalytics(app.config["DB_PATH"], read_only=True)
        rows = analytics.recent_mail_activity(
            account_email="primary@example.com",
            account_emails=["primary@example.com"],
            window_days=7,
            limit=10,
            reveal_pii=False,
        )
        rendered_labels = {row["from_label"] for row in rows}
        assert "alice@example.com" not in " ".join(rendered_labels)
        assert "bob@example.net" not in " ".join(rendered_labels)
        assert "a…@example.com" in rendered_labels
        assert "b…@example.net" in rendered_labels
