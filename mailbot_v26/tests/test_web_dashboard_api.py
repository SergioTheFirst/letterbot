from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import create_app


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str = "primary@example.com",
    payload: dict[str, object] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_type,
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            "entity",
            1,
            json.dumps(payload or {}, ensure_ascii=False),
            json.dumps(payload or {}, ensure_ascii=False),
            1,
            f"{event_type}-{ts_utc}",
        ),
    )


def test_api_dashboard_returns_json(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, priority, llm_provider, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "Invoice",
                "🔴",
                "gigachat",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO priority_feedback (id, email_id, kind, value, account_email, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "fb-1",
                "1",
                "priority_correction",
                "🔴",
                "primary@example.com",
                now.isoformat(),
            ),
        )
        _insert_event(
            conn,
            event_type="email_processed",
            ts_utc=now.timestamp(),
            payload={"text": "processed"},
        )
        _insert_event(
            conn,
            event_type="llm_fallback",
            ts_utc=now.timestamp(),
            payload={"reason": "timeout"},
        )
        _insert_event(
            conn, event_type="priority_correction_recorded", ts_utc=now.timestamp()
        )
        _insert_event(conn, event_type="surprise_detected", ts_utc=now.timestamp())
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, dict)
    assert data["emails_today"] >= 1
    assert data["emails_last_hour"] >= 1
    assert data["llm_calls_today"] >= 1
    assert data["llm_fallback_today"] >= 1
    assert data["priority"]["red"] >= 1
    assert data["corrections_week"] >= 1
    assert data["surprise_rate"] >= 0


def test_api_dashboard_returns_recent_events(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-events.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        tracked = [
            ("email_processed", {"text": "email processed"}),
            ("priority_classified", {"priority": "🟡"}),
            ("llm_summary_generated", {"text": "llm summary generated"}),
            ("priority_correction", {"priority": "🔴"}),
            ("pipeline_error", {"reason": "pipeline error"}),
        ]
        for offset, (event_type, payload) in enumerate(tracked):
            event_ts = (now - timedelta(minutes=offset)).timestamp()
            _insert_event(conn, event_type=event_type, ts_utc=event_ts, payload=payload)
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert [item["type"] for item in data["recent_events"]][:5] == [
        event for event, _ in tracked
    ]
    assert data["recent_events"][1]["text"] == "priority 🟡"


def test_api_dashboard_survives_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-empty.sqlite"
    sqlite3.connect(db_path).close()
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data == {
        "emails_today": 0,
        "emails_last_hour": 0,
        "llm_calls_today": 0,
        "llm_fallback_today": 0,
        "priority": {"red": 0, "yellow": 0, "blue": 0},
        "corrections_week": 0,
        "surprise_rate": 0.0,
        "recent_events": [],
        "top_contacts": [],
        "top_issuers": [],
        "interpretation": {"invoice_count": 0, "contract_count": 0, "invoice_total": 0},
        "business": {
            "payable_amount_total": 0,
            "payable_invoice_count": 0,
            "documents_waiting_attention_count": 0,
            "contract_review_count": 0,
            "reconciliation_attention_count": 0,
            "silence_risk_count": 0,
            "overdue_due_count": 0,
            "due_soon_count": 0,
        },
    }


def test_dashboard_template_renders_events_block(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-template.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/dashboard").get_data(as_text=True)

    assert 'data-testid="live-dashboard"' in body
    assert ">Recent events<" in body
    assert 'id="events-list"' in body
    assert 'id="business-payable"' in body
    assert 'id="top-issuers-list"' in body


def test_api_dashboard_limits_recent_events_to_20(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-events-limit.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for idx in range(25):
            event_ts = (now - timedelta(seconds=idx)).timestamp()
            _insert_event(
                conn,
                event_type="email_processed",
                ts_utc=event_ts,
                payload={"text": f"email processed {idx}"},
            )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["recent_events"]) == 20


def test_api_dashboard_returns_top_contacts(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-top-contacts.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, priority, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "Invoice",
                "invoice to pay",
                "🔴",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, body_summary, priority, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "Contract",
                "contract review",
                "🟡",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data["top_contacts"], list)
    assert data["top_contacts"]
    assert data["top_contacts"][0]["sender_email"] == "alice@example.com"


def test_dashboard_uses_interpretation_events_only(tmp_path: Path) -> None:
    test_dashboard_uses_interpretation_events(tmp_path)


def test_dashboard_uses_interpretation_not_raw_text(tmp_path: Path) -> None:
    test_dashboard_uses_interpretation_events(tmp_path)


def test_dashboard_uses_interpretation_events(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-interpretation.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=now.timestamp(),
            payload={
                "doc_kind": "invoice",
                "amount": 87500,
                "sender_email": "vendor@example.com",
                "due_date": "2026-04-15",
            },
        )
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=(now + timedelta(seconds=1)).timestamp(),
            payload={
                "doc_kind": "contract",
                "amount": None,
                "sender_email": "legal@example.com",
                "due_date": None,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["interpretation"]["invoice_count"] == 1
    assert data["interpretation"]["contract_count"] == 1
    assert data["interpretation"]["invoice_total"] == 87500


def test_api_dashboard_returns_business_metrics_from_interpretation_events(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-business.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=now.timestamp(),
            payload={
                "doc_kind": "invoice",
                "amount": 87500,
                "sender_email": "billing@vendor.example",
                "issuer_key": "domain:vendor.example",
                "issuer_label": "vendor.example",
                "issuer_domain": "vendor.example",
                "due_date": (now + timedelta(days=3)).strftime("%d.%m.%Y"),
                "confidence": 0.91,
                "action": "Проверить",
                "priority": "🟡",
            },
        )
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=(now + timedelta(seconds=1)).timestamp(),
            payload={
                "doc_kind": "contract",
                "amount": None,
                "sender_email": "legal@vendor.example",
                "issuer_key": "domain:vendor.example",
                "issuer_label": "vendor.example",
                "issuer_domain": "vendor.example",
                "due_date": None,
                "confidence": 0.88,
                "action": "Проверить договор",
                "priority": "🟡",
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["business"]["payable_amount_total"] == 87500
    assert data["business"]["payable_invoice_count"] == 1
    assert data["business"]["documents_waiting_attention_count"] == 2
    assert data["business"]["contract_review_count"] == 1
    assert data["business"]["due_soon_count"] == 1
    assert data["business"]["overdue_due_count"] == 0
    assert data["top_issuers"][0]["issuer_key"] == "domain:vendor.example"
    assert data["top_issuers"][0]["total_documents"] == 2
