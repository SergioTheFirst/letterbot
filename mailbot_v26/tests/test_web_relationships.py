import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


def _create_events_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE events_v1 (
                event_type TEXT,
                ts_utc REAL,
                account_id TEXT,
                entity_id TEXT,
                email_id INTEGER,
                payload TEXT,
                payload_json TEXT
            )
            """)


def _insert_event(
    db_path: Path,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str,
    entity_id: str,
    email_id: int | None,
    payload: dict[str, object] | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO events_v1 (event_type, ts_utc, account_id, entity_id, email_id, payload, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event_type,
                ts_utc,
                account_id,
                entity_id,
                email_id,
                json.dumps(payload or {}, ensure_ascii=False),
                json.dumps(payload or {}, ensure_ascii=False),
            ),
        )


def _build_relationships_app(tmp_path: Path) -> Path:
    db_path = tmp_path / "relationships.sqlite"
    _create_events_db(db_path)
    base_ts = datetime.now(timezone.utc) - timedelta(days=1)
    _insert_event(
        db_path,
        event_type="trust_score_updated",
        ts_utc=base_ts.timestamp(),
        account_id="primary@example.com",
        entity_id="contact-a",
        email_id=None,
        payload={"trust_score": 5.0, "entity_name": "Alpha"},
    )
    _insert_event(
        db_path,
        event_type="trust_score_updated",
        ts_utc=base_ts.timestamp() + 10,
        account_id="primary@example.com",
        entity_id="contact-a",
        email_id=None,
        payload={"trust_score": 6.0, "entity_name": "Alpha"},
    )
    _insert_event(
        db_path,
        event_type="trust_score_updated",
        ts_utc=base_ts.timestamp() + 5,
        account_id="primary@example.com",
        entity_id="contact-b",
        email_id=None,
        payload={"trust_score": 8.5, "entity_name": "Beta<script>"},
    )
    _insert_event(
        db_path,
        event_type="email_received",
        ts_utc=base_ts.timestamp() + 20,
        account_id="primary@example.com",
        entity_id="contact-a",
        email_id=1,
        payload={"subject": "should not leak"},
    )
    _insert_event(
        db_path,
        event_type="telegram_delivered",
        ts_utc=base_ts.timestamp() + 30,
        account_id="primary@example.com",
        entity_id="contact-b",
        email_id=2,
        payload={"raw_body": "forbidden"},
    )
    _insert_event(
        db_path,
        event_type="silence_signal_detected",
        ts_utc=base_ts.timestamp() + 40,
        account_id="primary@example.com",
        entity_id="contact-b",
        email_id=None,
        payload={},
    )
    _insert_event(
        db_path,
        event_type="email_received",
        ts_utc=base_ts.timestamp(),
        account_id="other@example.com",
        entity_id="contact-c",
        email_id=3,
        payload={"body_text": "must stay hidden"},
    )
    return db_path


def test_relationships_page_and_api_accessible_without_login(tmp_path: Path) -> None:
    db_path = _build_relationships_app(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        response = client.get(
            "/relationships", query_string={"account_email": "primary@example.com"}
        )
        assert response.status_code == 200
        api_graph = client.get(
            "/api/v1/relationships/graph",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_graph.status_code == 200
        api_contact = client.get(
            "/api/v1/relationships/contact",
            query_string={
                "account_email": "primary@example.com",
                "contact_id": "contact:contact-a",
            },
        )
        assert api_contact.status_code == 200


def test_relationships_deterministic_and_sorted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _build_relationships_app(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        query = {
            "account_email": "primary@example.com",
            "window_days": "30",
            "limit": "50",
        }
        first = client.get("/api/v1/relationships/graph", query_string=query).get_json()
        second = client.get(
            "/api/v1/relationships/graph", query_string=query
        ).get_json()
        assert first == second
        nodes = [node for node in first["nodes"] if node["id"] != "user:me"]
        assert [node["id"] for node in nodes] == [
            "contact:contact-b",
            "contact:contact-a",
        ]
        assert nodes[0]["trust_score"] == 8.5
        assert "trust_delta" not in nodes[0]
        assert nodes[1]["trust_delta"] == 1.0


def test_relationships_scope_isolation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _build_relationships_app(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        query = {
            "account_email": "other@example.com",
            "account_emails": "other@example.com",
        }
        response = client.get("/api/v1/relationships/graph", query_string=query)
        assert response.status_code == 200
        data = response.get_json()
        contact_ids = [node["id"] for node in data["nodes"] if node["id"] != "user:me"]
        assert contact_ids == ["contact:contact-c"]


def test_relationships_pii_and_escaping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _build_relationships_app(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    forbidden = [
        "raw_body",
        "body_text",
        "subject",
        "forbidden",
        "should not leak",
        "must stay hidden",
        "Beta<script>",
    ]
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        api_response = client.get(
            "/api/v1/relationships/graph",
            query_string={"account_email": "primary@example.com"},
        )
        text = api_response.get_data(as_text=True)
        for token in forbidden:
            assert token not in text
        page_response = client.get(
            "/relationships", query_string={"account_email": "primary@example.com"}
        )
        page_text = page_response.get_data(as_text=True)
    for token in forbidden:
        assert token not in page_text


def test_relationships_window_validation(tmp_path: Path) -> None:
    db_path = _build_relationships_app(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        default_resp = client.get(
            "/api/v1/relationships/graph",
            query_string={"account_email": "primary@example.com"},
        )
        assert default_resp.status_code == 200
        payload = default_resp.get_json()
        assert payload["scope"]["window_days"] == 30
        invalid_resp = client.get(
            "/api/v1/relationships/graph",
            query_string={"account_email": "primary@example.com", "window_days": "5"},
        )
        assert invalid_resp.status_code == 400
