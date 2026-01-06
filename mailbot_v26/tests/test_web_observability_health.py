import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.web_observability.app import create_app


def _insert_health_snapshot(
    db_path: Path,
    *,
    snapshot_id: str,
    ts_utc: float,
    gates_state: str,
    metrics_brief: str,
    system_mode: str,
    account_id: str,
) -> None:
    payload_json = json.dumps({"system_mode": system_mode}, ensure_ascii=False)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO system_health_snapshots (
                snapshot_id, ts_utc, payload_json, gates_state, metrics_brief, system_mode
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                ts_utc,
                payload_json,
                gates_state,
                metrics_brief,
                system_mode,
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO processing_spans (
                span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
                stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
                fallback_used, outcome, error_code, health_snapshot_id, delivery_mode,
                wait_budget_seconds, elapsed_to_first_send_ms, edit_applied
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, 0, 'ok', '', ?, '', 0, 0, 0)
            """,
            (
                f"span-{snapshot_id}",
                ts_utc - 1,
                ts_utc,
                100,
                account_id,
                json.dumps({}, ensure_ascii=False),
                snapshot_id,
            ),
        )


def _build_app_with_health_data(tmp_path: Path) -> tuple[Path, object]:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc) - timedelta(days=1)
    _insert_health_snapshot(
        db_path,
        snapshot_id="h1",
        ts_utc=now.timestamp(),
        gates_state=json.dumps({"passed": True, "failed": []}, ensure_ascii=False),
        metrics_brief=json.dumps({"telegram_delivery_success_rate": 0.99}, ensure_ascii=False),
        system_mode="FULL",
        account_id="primary@example.com",
    )
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    return db_path, app


def test_health_auth_required(tmp_path: Path) -> None:
    _, app = _build_app_with_health_data(tmp_path)
    with app.test_client() as client:
        response = client.get("/health")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")

        api_response = client.get("/api/v1/observability/health_timeline")
        assert api_response.status_code == 302
        assert "/login" in api_response.headers.get("Location", "")


def test_health_default_window_selected_and_api(tmp_path: Path) -> None:
    _, app = _build_app_with_health_data(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})

        page = client.get(
            "/health", query_string={"account_email": "primary@example.com"}
        )
        assert page.status_code == 200
        body = page.get_data(as_text=True)
        assert '<option value="30" selected>' in body

        api_response = client.get(
            "/api/v1/observability/health_timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_response.status_code == 200
        payload = api_response.get_json()
        assert payload["window_days"] == 30


def test_health_payload_resilience_and_sanitization(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    ProcessingSpanRecorder(db_path)
    ts_utc = (datetime.now(timezone.utc) - timedelta(days=2)).timestamp()
    _insert_health_snapshot(
        db_path,
        snapshot_id="broken",
        ts_utc=ts_utc,
        gates_state="not-json",
        metrics_brief="{invalid",
        system_mode="",
        account_id="primary@example.com",
    )
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})

        page = client.get(
            "/health", query_string={"account_email": "primary@example.com"}
        )
        assert page.status_code == 200

        api_response = client.get(
            "/api/v1/observability/health_timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_response.status_code == 200
        payload = api_response.get_json()
        assert payload is not None
        assert payload.get("timeline")
        timeline_entry = payload["timeline"][0]
        assert "payload_json" not in json.dumps(timeline_entry, ensure_ascii=False)
        assert isinstance(timeline_entry.get("gates_state"), dict)
        assert isinstance(timeline_entry.get("metrics_brief"), dict)
