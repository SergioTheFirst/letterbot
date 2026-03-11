import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


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


def _insert_archive_interpretation(
    db_path: Path,
    *,
    email_id: int,
    account_email: str,
    from_email: str,
    subject: str,
    received_at: datetime,
    issuer_label: str | None = None,
    doc_kind: str = "invoice",
    amount: int | None = 12500,
    due_date: str | None = "2026-04-15",
    action: str = "Проверить",
    priority: str = "yellow",
    confidence: float = 0.86,
    document_id: str = "INV-001",
) -> None:
    _insert_event(
        db_path,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=received_at.timestamp(),
        account_id=account_email,
        email_id=email_id,
        payload={"from_email": from_email, "subject": subject},
    )
    _insert_event(
        db_path,
        event_type=EventType.MESSAGE_INTERPRETATION,
        ts_utc=received_at.timestamp(),
        account_id=account_email,
        email_id=email_id,
        payload={
            "sender_email": from_email,
            "doc_kind": doc_kind,
            "amount": amount,
            "due_date": due_date,
            "priority": priority,
            "action": action,
            "confidence": confidence,
            "context": "NEW_MESSAGE",
            "document_id": document_id,
            "issuer_label": issuer_label or "Vendor Ops",
            "issuer_key": "domain:example.com",
            "issuer_domain": "example.com",
        },
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


def test_archive_page_accessible_without_login_and_headers(tmp_path: Path) -> None:
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
    _insert_archive_interpretation(
        db_path,
        email_id=1,
        account_email="acct@example.com",
        from_email="sender@example.com",
        subject="Invoice 1",
        received_at=now,
        issuer_label="Vendor Ops",
    )

    with app.test_client() as client:
        response = client.get("/archive")
        assert response.status_code == 200
        page = response
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert "Priority" in body
        assert "Doc kind" in body
        assert "Confidence" in body


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
        _insert_archive_interpretation(
            db_path,
            email_id=email_id,
            account_email="acct@example.com",
            from_email=f"sender{email_id}@example.com",
            subject=f"Invoice {email_id}",
            received_at=now.replace(microsecond=email_id),
            issuer_label=f"Vendor {email_id}",
            document_id=f"INV-{email_id}",
        )

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get(
            "/archive", query_string={"account_emails": "acct@example.com"}
        )
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
        login_with_csrf(client, "pw")
        page = client.get("/email/42")
        body = page.get_data(as_text=True)
        stages = re.findall(r'data-stage="([^"]+)"', body)
        assert stages[:3] == ["analyze", "parse", "send"]


def test_cockpit_renders_flat_text_instead_of_serialized_repr(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    now = datetime.now(timezone.utc)
    _insert_email(
        db_path,
        email_id=77,
        account_email="acct@example.com",
        from_email="sender@example.com",
        received_at=now,
    )

    class FakeAnalytics:
        def cockpit_summary(self, **kwargs):
            return {
                "status_strip": {},
                "today_digest": {"items": []},
                "week_digest": {"items": []},
                "golden_signals": {},
                "engineer": {},
                "recent_activity": [],
            }

        def lane_activity_rows(self, **kwargs):
            return []

        def lane_counts(self, **kwargs):
            return {
                "all": 1,
                "critical": 0,
                "commitments": 0,
                "deferred": 0,
                "failures": 0,
                "learning": 0,
            }

        def cockpit_top_senders(self, *args, **kwargs):
            return [{"display_name": ["Alice", ["Ops"]], "count": 3}]

        def cockpit_silent_contacts(self, *args, **kwargs):
            return [{"display_name": ["Bob", ["Vendor"]], "days_silent": 10}]

        def cockpit_stalled_threads(self, *args, **kwargs):
            return [
                {
                    "from_email": ["sender@example.com"],
                    "subject": ['["Invoice"]'],
                    "snippet": [["Need action"]],
                    "days_ago": 2,
                }
            ]

        def commitment_status_counts(self, **kwargs):
            return {"pending": 0}

    app.config["ANALYTICS_FACTORY"] = lambda: FakeAnalytics()

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/")
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert ">Live feed<" in body
        assert "[&#39;[" not in body


def test_archive_page_shows_rows_for_existing_db_emails(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    now = datetime.now(timezone.utc)
    _insert_email(
        db_path,
        email_id=11,
        account_email="acct@example.com",
        from_email="sender@example.com",
        received_at=now,
        action_line="Проверить",
        body_summary="Короткий текст",
    )
    _insert_archive_interpretation(
        db_path,
        email_id=11,
        account_email="acct@example.com",
        from_email="sender@example.com",
        subject="Invoice 11",
        received_at=now,
        issuer_label="Vendor Ops",
    )

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get(
            "/archive", query_string={"account_emails": "acct@example.com"}
        )
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert 'data-email-id="11"' in body
        assert "No archive entries found." not in body


def test_archive_empty_state_human_readable(tmp_path: Path) -> None:
    _, app = _build_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get(
            "/archive", query_string={"account_emails": "unknown@example.com"}
        )
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert "No archive entries found." in body
        assert "Try a wider window or clear one of the archive filters." in body
