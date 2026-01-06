import json
import sqlite3
from pathlib import Path

import pytest

from mailbot_v26.web_observability.app import create_app


def _prepare_events_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE events_v1 (
                event_type TEXT,
                ts_utc REAL,
                account_id TEXT,
                entity_id TEXT,
                email_id INTEGER,
                payload TEXT,
                payload_json TEXT
            )
            """
        )
        rows = [
            (
                "delivery_policy_applied",
                200.0,
                "primary@example.com",
                "entity-b",
                3,
                None,
                json.dumps(
                    {
                        "priority": "🔴",
                        "confidence": 90,
                        "subject": "PII should be hidden",
                        "sender": "sensitive@example.com",
                    },
                    ensure_ascii=False,
                ),
            ),
            (
                "attention_deferred_for_digest",
                200.0,
                "primary@example.com",
                "entity-a",
                2,
                None,
                json.dumps(
                    {
                        "priority": "🟡",
                        "delivery_mode": "digest",
                        "body_text": "secret body",
                    },
                    ensure_ascii=False,
                ),
            ),
            (
                "surprise_detected",
                150.0,
                "primary@example.com",
                "entity-c",
                1,
                None,
                json.dumps(
                    {
                        "confidence_score": 0.42,
                        "decision": "shadow",
                        "raw": "should not leak",
                    },
                    ensure_ascii=False,
                ),
            ),
            (
                "delivery_policy_applied",
                100.0,
                "other@example.com",
                "entity-z",
                9,
                None,
                json.dumps({"priority": "🔵"}, ensure_ascii=False),
            ),
        ]
        conn.executemany(
            "INSERT INTO events_v1 (event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _build_app(tmp_path: Path):
    db_path = tmp_path / "events.sqlite"
    _prepare_events_db(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_events_auth_required(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/events")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")


def test_events_api_ordering_and_scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _build_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        query = {
            "account_email": "primary@example.com",
            "account_emails": "primary@example.com",
            "limit": "200",
        }
        first = client.get("/api/v1/events/timeline", query_string=query)
        second = client.get("/api/v1/events/timeline", query_string=query)
        assert first.status_code == 200
        assert second.status_code == 200
        payload_first = first.get_json()
        payload_second = second.get_json()
        assert payload_first == payload_second
        items = payload_first["items"]
        assert len(items) == 3
        assert [item["event_type"] for item in items] == [
            "attention_deferred_for_digest",
            "delivery_policy_applied",
            "surprise_detected",
        ]
        account_ids = {item.get("email_id") for item in items}
        assert 9 not in account_ids


def test_events_pii_scrubbed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _build_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    forbidden = ["PII should be hidden", "sensitive@example.com", "secret body", "should not leak"]
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        api_response = client.get(
            "/api/v1/events/timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_response.status_code == 200
        text = api_response.get_data(as_text=True)
        for token in forbidden:
            assert token not in text
        page_response = client.get(
            "/events",
            query_string={"account_email": "primary@example.com"},
        )
        assert page_response.status_code == 200
        page_text = page_response.get_data(as_text=True)
        for token in forbidden:
            assert token not in page_text


def test_events_default_window(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _build_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        response = client.get(
            "/api/v1/events/timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["window_days"] == 30
        page_response = client.get("/events", query_string={"account_email": "primary@example.com"})
        assert page_response.status_code == 200
        assert 'value="30" selected' in page_response.get_data(as_text=True)
