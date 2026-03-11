from __future__ import annotations

import json
import socket
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import _dashboard_health_view, create_app


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str = "primary@example.com",
    email_id: int = 1,
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
            email_id,
            json.dumps(payload or {}, ensure_ascii=False),
            json.dumps(payload or {}, ensure_ascii=False),
            1,
            f"{event_type}-{ts_utc}",
        ),
    )


def _insert_archive_interpretation(
    conn: sqlite3.Connection,
    *,
    email_id: int,
    ts_utc: float,
    account_id: str = "primary@example.com",
    sender_email: str = "sender@example.com",
    issuer_label: str = "Vendor Ops",
    subject: str = "Invoice",
    doc_kind: str = "invoice",
    amount: int | None = 87500,
    due_date: str | None = "2026-04-15",
    action: str = "Проверить",
    priority: str = "yellow",
    confidence: float = 0.91,
    document_id: str = "INV-001",
    body_summary: str = "",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO emails (id, account_email, from_email, subject, body_summary, received_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            email_id,
            account_id,
            sender_email,
            subject,
            body_summary,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
        ),
    )
    _insert_event(
        conn,
        event_type="email_received",
        ts_utc=ts_utc,
        account_id=account_id,
        email_id=email_id,
        payload={"from_email": sender_email, "subject": subject},
    )
    _insert_event(
        conn,
        event_type="message_interpretation",
        ts_utc=ts_utc,
        account_id=account_id,
        email_id=email_id,
        payload={
            "sender_email": sender_email,
            "doc_kind": doc_kind,
            "amount": amount,
            "due_date": due_date,
            "priority": priority,
            "action": action,
            "confidence": confidence,
            "context": "NEW_MESSAGE",
            "document_id": document_id,
            "issuer_label": issuer_label,
            "issuer_key": "domain:vendor.example",
            "issuer_domain": "vendor.example",
        },
    )


def _insert_health_snapshot(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    ts_utc: float,
    system_mode: str = "FULL",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO system_health_snapshots (
            snapshot_id, ts_utc, payload_json, gates_state, metrics_brief, system_mode
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            ts_utc,
            json.dumps({"system_mode": system_mode}, ensure_ascii=False),
            json.dumps({"db": "ok"}, ensure_ascii=False),
            json.dumps({"telegram_delivery_success_rate": 0.99}, ensure_ascii=False),
            system_mode,
        ),
    )


def _insert_processing_span(
    conn: sqlite3.Connection,
    *,
    span_id: str,
    ts_start_utc: float,
    ts_end_utc: float,
    account_id: str = "primary@example.com",
    email_id: int = 1,
    health_snapshot_id: str = "snap-1",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO processing_spans (
            span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
            stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
            fallback_used, outcome, error_code, health_snapshot_id, delivery_mode,
            wait_budget_seconds, elapsed_to_first_send_ms, edit_applied
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            span_id,
            ts_start_utc,
            ts_end_utc,
            int(max((ts_end_utc - ts_start_utc) * 1000.0, 1.0)),
            account_id,
            email_id,
            json.dumps({"parse": 20, "llm": 180}, ensure_ascii=False),
            "gigachat",
            "giga-pro",
            180,
            0.92,
            0,
            "ok",
            "",
            health_snapshot_id,
            "direct",
            0,
            0,
            0,
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


def test_api_dashboard_includes_observability_sections_with_runtime_data(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-observability.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, priority, llm_provider, received_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "primary@example.com",
                "alice@example.com",
                "Invoice",
                "🔴",
                "gigachat",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        _insert_health_snapshot(conn, snapshot_id="snap-1", ts_utc=now.timestamp())
        _insert_processing_span(
            conn,
            span_id="span-1",
            ts_start_utc=(now - timedelta(milliseconds=250)).timestamp(),
            ts_end_utc=now.timestamp(),
        )
        _insert_event(
            conn,
            event_type="imap_health",
            ts_utc=now.timestamp(),
            payload={"subtype": "success"},
        )
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=now.timestamp(),
            payload={"doc_kind": "invoice"},
        )
        _insert_event(conn, event_type="telegram_delivered", ts_utc=now.timestamp())
        _insert_event(
            conn,
            event_type="DECISION_TRACE_RECORDED",
            ts_utc=now.timestamp(),
            payload={
                "decision_key": "trace-1",
                "decision_kind": "priority",
                "anchor_ts_utc": now.timestamp(),
                "signals_evaluated": ["INVOICE_KEYWORD"],
                "signals_fired": ["INVOICE_KEYWORD"],
                "evidence": {"matched": 1, "total": 1},
                "model_fingerprint": "model-1",
                "explain_codes": ["INVOICE_KEYWORD"],
                "trace_schema": "DecisionTraceV1",
                "trace_version": 1,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    data = resp.get_json()
    assert resp.status_code == 200
    assert data["latency"]["status"] == "ok"
    assert data["latency"]["sample_count"] == 1
    assert data["health"]["status"] in {"ok", "partial"}
    assert data["ai"]["status"] == "ok"
    assert data["ai"]["trace_coverage"] == "1/1 (100%)"
    assert data["ai"]["recent_traces"][0]["decision_kind"] == "priority"


def test_api_dashboard_observability_sections_stay_honest_without_data(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-observability-empty.sqlite"
    sqlite3.connect(db_path).close()
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    data = resp.get_json()
    assert resp.status_code == 200
    assert data["latency"]["status"] == "unknown"
    assert data["latency"]["status_label"] == "NO LATENCY DATA"
    assert data["health"]["status"] == "unknown"
    assert data["ai"]["status"] == "unavailable"
    assert data["meta"]["sections"]["latency"]["status"] == "unknown"
    assert data["meta"]["sections"]["health"]["status"] == "unknown"
    assert data["meta"]["sections"]["ai"]["status"] == "unavailable"


def test_dashboard_health_view_treats_auxiliary_telegram_down_as_degraded() -> None:
    payload = {
        "components": [
            {"name": "IMAP", "status": "ok"},
            {"name": "DB", "status": "ok"},
            {"name": "Scheduler / Digests", "status": "ok"},
            {"name": "Telegram", "status": "down"},
            {"name": "LLM", "status": "ok"},
        ]
    }

    health = _dashboard_health_view(payload)

    assert health["status"] == "degraded"
    assert health["status_label"] == "DEGRADED"


def test_api_dashboard_survives_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-empty.sqlite"
    sqlite3.connect(db_path).close()
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["emails_today"] is None
    assert data["emails_last_hour"] is None
    assert data["llm_calls_today"] is None
    assert data["llm_fallback_today"] is None
    assert data["priority"] == {"red": None, "yellow": None, "blue": None}
    assert data["corrections_week"] is None
    assert data["surprise_rate"] is None
    assert data["recent_events"] == []
    assert data["top_contacts"] == []
    assert data["top_issuers"] == []
    assert data["interpretation"] == {
        "invoice_count": None,
        "contract_count": None,
        "invoice_total": None,
    }
    assert data["business"] == {
        "payable_amount_total": None,
        "payable_invoice_count": None,
        "documents_waiting_attention_count": None,
        "contract_review_count": None,
        "reconciliation_attention_count": None,
        "silence_risk_count": None,
        "overdue_due_count": None,
        "due_soon_count": None,
    }
    assert data["meta"]["status"] == "partial"
    assert data["meta"]["sections"]["emails"]["status"] == "unavailable"
    assert data["meta"]["sections"]["events"]["status"] == "unavailable"
    assert data["meta"]["sections"]["business"]["status"] == "partial"


def test_api_dashboard_survives_legacy_emails_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-legacy.sqlite"
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_email TEXT NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                from_email TEXT,
                from_name TEXT,
                subject TEXT,
                received_at TEXT,
                attachments_count INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                error_last TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                telegram_delivered_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE priority_feedback (
                id TEXT PRIMARY KEY,
                email_id TEXT,
                kind TEXT,
                value TEXT,
                account_email TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE events_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                ts_utc REAL,
                ts TEXT,
                account_id TEXT,
                entity_id TEXT,
                email_id TEXT,
                payload TEXT,
                payload_json TEXT,
                schema_version INTEGER,
                fingerprint TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO emails (
                account_email, uid, message_id, from_email, from_name, subject,
                received_at, status, error_last, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                1,
                "<m1>",
                "alice@example.com",
                "Alice",
                "Legacy invoice",
                now.isoformat(),
                "NEW",
                "",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        _insert_event(
            conn,
            event_type="email_processed",
            ts_utc=now.timestamp(),
            account_id="primary@example.com",
            email_id=1,
            payload={"text": "processed"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["emails_today"] == 1
    assert data["llm_calls_today"] is None
    assert data["priority"] == {"red": None, "yellow": None, "blue": None}
    assert data["recent_events"][0]["type"] == "email_processed"
    assert data["meta"]["status"] == "partial"
    assert data["meta"]["sections"]["llm"]["status"] == "partial"
    assert data["meta"]["sections"]["priority"]["status"] == "partial"


def test_dashboard_template_renders_events_block(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-template.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/dashboard").get_data(as_text=True)

    assert 'data-testid="live-dashboard"' in body
    assert ">Recent events<" in body
    assert 'id="preview-recent-events"' in body
    assert 'id="processed-table-body"' in body
    assert 'id="top-issuers-list"' in body
    assert 'id="card-ops-health"' in body
    assert 'id="health-imap-status"' in body
    assert 'id="health-pipeline-status"' in body
    assert 'id="dashboard-meta-status"' in body
    assert 'id="dashboard-meta-detail"' in body


def test_api_dashboard_invalidates_cache_after_worker_write(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-cache-refresh.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, received_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "First",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        first = client.get("/api/dashboard").get_json()
        with sqlite3.connect(db_path) as conn:
            later = now + timedelta(minutes=1)
            conn.execute(
                """
                INSERT INTO emails (account_email, from_email, subject, received_at, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "primary@example.com",
                    "bob@example.com",
                    "Second",
                    later.isoformat(),
                    later.isoformat(),
                ),
            )
            conn.commit()
        second = client.get("/api/dashboard").get_json()

    assert first["emails_today"] == 1
    assert second["emails_today"] == 2
    assert second["meta"]["status"] == "partial" or second["meta"]["status"] == "ok"


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


def test_health_panel_imap_status_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-health-ok.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={"subtype": "success", "detail": "ok"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["dead_letter_count"] == 0


def test_health_panel_imap_status_degraded_when_no_recent_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-health-degraded.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=(now - timedelta(minutes=20)).timestamp(),
            payload={"subtype": "success", "detail": "stale"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    assert resp.get_json()["status"] == "degraded"


def test_health_panel_imap_status_down_when_no_success_over_threshold(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-health-down.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            payload={"subtype": "success", "detail": "old"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    assert resp.get_json()["status"] == "down"


def test_health_panel_imap_status_unknown_without_events(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-health-unknown.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "unknown"
    assert data["last_success_ts"] is None
    assert data["dead_letter_count"] == 0


def test_health_panel_pipeline_last_processed_correct(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-pipeline-last-processed.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    last_processed_ts = (now - timedelta(minutes=3)).timestamp()
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=last_processed_ts,
            payload={
                "doc_kind": "invoice",
                "amount": 1000,
                "sender_email": "vendor@example.com",
                "action": "Проверить",
                "priority": "🟡",
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/pipeline")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["last_processed_ts"] is not None


def test_health_panel_dead_letter_count_correct(tmp_path: Path) -> None:
    db_path = tmp_path / "dashboard-dead-letters.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={"subtype": "dead_letter", "detail": "parse failed"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    assert resp.get_json()["dead_letter_count"] == 1


def test_health_panel_pending_actions_count_from_canonical_events(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dashboard-pending-actions.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=now.timestamp(),
            payload={
                "doc_kind": "contract",
                "amount": None,
                "sender_email": "legal@example.com",
                "action": "Проверить договор",
                "priority": "🟡",
                "confidence": 0.88,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/pipeline")

    assert resp.status_code == 200
    assert resp.get_json()["pending_action_count"] == 1


def test_health_status_components_unknown_without_runtime_evidence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "health-status-unknown.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert components["IMAP"]["status"] == "unknown"
    assert components["Telegram"]["status"] == "unknown"
    assert components["DB"]["status"] == "unknown"
    assert components["LLM"]["status"] == "unknown"
    assert components["Scheduler / Digests"]["status"] == "unknown"


def test_archive_filter_by_priority_returns_correct_items(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-priority.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=1,
            ts_utc=now.timestamp(),
            priority="red",
            issuer_label="Vendor High",
        )
        _insert_archive_interpretation(
            conn,
            email_id=2,
            ts_utc=(now - timedelta(minutes=1)).timestamp(),
            priority="blue",
            issuer_label="Vendor Low",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={"account_emails": "primary@example.com", "priority": "high"},
        )

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["items"][0]["sender_display"] == "Vendor High"


def test_archive_filter_by_doc_kind_returns_correct_items(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-doc-kind.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=1,
            ts_utc=now.timestamp(),
            doc_kind="invoice",
            issuer_label="Invoice Vendor",
        )
        _insert_archive_interpretation(
            conn,
            email_id=2,
            ts_utc=(now - timedelta(minutes=1)).timestamp(),
            doc_kind="contract",
            issuer_label="Legal Vendor",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={"account_emails": "primary@example.com", "doc_kind": "contract"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["total"] == 1
    assert payload["items"][0]["doc_kind"] == "contract"


def test_archive_filter_by_confidence_band(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-confidence.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=1,
            ts_utc=now.timestamp(),
            confidence=0.92,
            issuer_label="High Confidence",
        )
        _insert_archive_interpretation(
            conn,
            email_id=2,
            ts_utc=(now - timedelta(minutes=1)).timestamp(),
            confidence=0.41,
            issuer_label="Low Confidence",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={
                "account_emails": "primary@example.com",
                "confidence_band": "low",
            },
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["total"] == 1
    assert payload["items"][0]["confidence_text"] == "0.41"


def test_archive_filters_combinable_without_error(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-combined.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=1,
            ts_utc=now.timestamp(),
            sender_email="billing@vendor.example",
            issuer_label="Vendor Ops",
            doc_kind="invoice",
            priority="red",
            confidence=0.88,
        )
        _insert_archive_interpretation(
            conn,
            email_id=2,
            ts_utc=(now - timedelta(minutes=1)).timestamp(),
            sender_email="legal@vendor.example",
            issuer_label="Legal Vendor",
            doc_kind="contract",
            priority="yellow",
            confidence=0.76,
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={
                "account_emails": "primary@example.com",
                "sender": "vendor ops",
                "priority": "high",
                "doc_kind": "invoice",
                "confidence_band": "high",
            },
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["total"] == 1
    assert payload["items"][0]["message_id"] == 1


def test_archive_pagination_limits_to_25_per_page(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-pagination.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for idx in range(30):
            _insert_archive_interpretation(
                conn,
                email_id=idx + 1,
                ts_utc=(now - timedelta(minutes=idx)).timestamp(),
                issuer_label=f"Vendor {idx + 1}",
                document_id=f"INV-{idx + 1}",
            )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert len(payload["items"]) == 25
    assert payload["pages"] == 2


def test_archive_detail_returns_interpretation_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-detail-summary.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(conn, email_id=7, ts_utc=now.timestamp())
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive/7/detail",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert "Invoice" in payload["interpretation_summary"]


def test_archive_detail_returns_why_classified(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-detail-why.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(conn, email_id=9, ts_utc=now.timestamp())
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive/9/detail",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert "Classified because detected" in payload["why_classified"]


def test_archive_detail_no_raw_body_reparse(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-detail-canonical.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=11,
            ts_utc=now.timestamp(),
            body_summary="RAW BODY GARBAGE SHOULD NOT APPEAR",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive/11/detail",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    dumped = json.dumps(payload, ensure_ascii=False)
    assert "RAW BODY GARBAGE" not in dumped


def test_archive_sender_display_uses_issuer_identity_not_raw_email(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-sender-identity.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_archive_interpretation(
            conn,
            email_id=12,
            ts_utc=now.timestamp(),
            sender_email="raw-sender@example.com",
            issuer_label="ООО Vendor",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["items"][0]["sender_display"] == "ООО Vendor"


def test_archive_empty_filters_returns_all_items_paginated(tmp_path: Path) -> None:
    db_path = tmp_path / "archive-empty-filters.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for idx in range(3):
            _insert_archive_interpretation(
                conn,
                email_id=idx + 1,
                ts_utc=(now - timedelta(minutes=idx)).timestamp(),
                issuer_label=f"Vendor {idx + 1}",
            )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/archive",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["total"] == 3
    assert len(payload["items"]) == 3


def test_health_status_ok_when_recent_success(tmp_path: Path) -> None:
    db_path = tmp_path / "health-status-ok.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={"subtype": "success", "detail": "ok"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert components["IMAP"]["status"] == "ok"
    assert datetime.fromisoformat(str(components["IMAP"]["last_ok"]))


def test_health_status_degraded_when_no_success_10_to_60_min(tmp_path: Path) -> None:
    db_path = tmp_path / "health-status-degraded.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=(now - timedelta(minutes=20)).timestamp(),
            payload={"subtype": "success", "detail": "stale"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert components["IMAP"]["status"] == "degraded"


def test_health_status_down_when_no_success_over_60_min(tmp_path: Path) -> None:
    db_path = tmp_path / "health-status-down.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=(now - timedelta(hours=2)).timestamp(),
            payload={"subtype": "success", "detail": "old"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert components["IMAP"]["status"] == "down"


def test_health_status_keeps_db_snapshot_signal_without_false_down_on_idle(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "health-status-idle-snapshot.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_health_snapshot(
            conn,
            snapshot_id="snap-idle",
            ts_utc=(now - timedelta(hours=2)).timestamp(),
        )
        _insert_processing_span(
            conn,
            span_id="span-idle",
            ts_start_utc=(now - timedelta(hours=2, minutes=1)).timestamp(),
            ts_end_utc=(now - timedelta(hours=2)).timestamp(),
            health_snapshot_id="snap-idle",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert components["DB"]["status"] == "ok"


def test_health_status_shows_human_readable_cause_not_traceback(tmp_path: Path) -> None:
    db_path = tmp_path / "health-status-cause.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=(now - timedelta(minutes=20)).timestamp(),
            payload={"subtype": "success", "detail": "stale"},
        )
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={
                "subtype": "processing_failure",
                "detail": "Traceback: socket timeout during IMAP login",
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    components = {row["name"]: row for row in resp.get_json()["components"]}
    assert "Traceback" not in (components["IMAP"]["detail"] or "")
    assert "Ошибка" in (components["IMAP"]["detail"] or "")


def test_health_cooldown_active_when_cooldown_event_present(tmp_path: Path) -> None:
    db_path = tmp_path / "health-cooldown-active.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    resume_at = (now + timedelta(minutes=23)).isoformat()
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={
                "subtype": "cooldown",
                "detail": "cooldown active",
                "cooldown_resume_at": resume_at,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    payload = resp.get_json()
    assert payload["cooldown_active"] is True
    assert payload["cooldown_resume_at"] == resume_at


def test_health_cooldown_resume_time_correct(tmp_path: Path) -> None:
    db_path = tmp_path / "health-cooldown-resume.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    resume_at = (now + timedelta(minutes=23)).isoformat()
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={
                "subtype": "cooldown",
                "detail": "cooldown active",
                "cooldown_resume_at": resume_at,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    payload = resp.get_json()
    assert payload["cooldown_resume_at"] == resume_at
    assert payload["cooldown_resume_relative"]


def test_health_all_configured_components_present(tmp_path: Path) -> None:
    db_path = tmp_path / "health-components.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    names = {row["name"] for row in resp.get_json()["components"]}
    assert names == {"IMAP", "Telegram", "DB", "LLM", "Scheduler / Digests"}


def test_health_no_live_network_calls_on_load(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "health-no-network.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    def _fail(*args, **kwargs):
        raise AssertionError("network call attempted")

    monkeypatch.setattr(socket, "create_connection", _fail)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get(
            "/api/health/status",
            query_string={"account_emails": "primary@example.com"},
        )

    assert resp.status_code == 200


def test_health_traceback_hidden_by_default(tmp_path: Path) -> None:
    db_path = tmp_path / "health-traceback-hidden.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type="imap_health_v1",
            ts_utc=now.timestamp(),
            payload={
                "subtype": "processing_failure",
                "detail": "Traceback: auth timeout",
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/health?account_emails=primary@example.com")

    body = page.get_data(as_text=True)
    assert "Traceback: auth timeout" not in body
    assert "data-testid=\"health-component-matrix\"" in body
