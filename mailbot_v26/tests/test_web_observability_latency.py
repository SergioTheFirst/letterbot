import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
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
