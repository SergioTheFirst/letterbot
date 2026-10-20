"""Web health shows unconfirmed Telegram deliveries (WP1a follow-up).

An accepted-then-timeout send becomes DELIVERY_UNKNOWN and is never resent.
The runtime records it as a TELEGRAM_FAILED contract event with
delivery_state "unknown" - the evidence NotificationSLAResult.unknown_24h and
the Telegram /status line count. The web health page and /api/health/status
surface the 24h count next to the Telegram component, as an aggregate only.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"
OTHER_ACCOUNT = "other@example.com"
SECRET_SUBJECT = "SECRET-SUBJECT-DU9"
SECRET_SENDER = "secret.du@example.com"
UNKNOWN_EMAIL_ID = 4242
COUNT_MARKER = 'data-testid="telegram-delivery-unknown"'


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _seed_email(db_path: Path, *, email_id: int, account: str = ACCOUNT) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, received_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                email_id,
                account,
                SECRET_SENDER,
                SECRET_SUBJECT,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def _emit(
    db_path: Path,
    *,
    event_type: EventType,
    email_id: int,
    ts_utc: float,
    payload: dict[str, object],
    account: str = ACCOUNT,
) -> None:
    emitted = EventEmitter(db_path).emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account,
            entity_id=None,
            email_id=email_id,
            payload=payload,
        )
    )
    assert emitted


def _unknown_payload(ts_utc: float) -> dict[str, object]:
    # Same shape as processor._report_delivery_unknown writes.
    return {
        "delivered": False,
        "error": "delivery_unknown",
        "delivery_state": "unknown",
        "error_class": "ReadTimeout",
        "occurred_at_utc": ts_utc,
        "mode": "html",
        "retry_count": 0,
    }


def _seed_delivery_unknown(
    db_path: Path, *, email_id: int, ts_utc: float, account: str = ACCOUNT
) -> None:
    _seed_email(db_path, email_id=email_id, account=account)
    _emit(
        db_path,
        event_type=EventType.TELEGRAM_FAILED,
        email_id=email_id,
        ts_utc=ts_utc,
        payload=_unknown_payload(ts_utc),
        account=account,
    )


def _build_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "health-delivery-unknown.sqlite"
    KnowledgeDB(db_path)
    return db_path


def _app(db_path: Path):
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _status_payload(client, account: str = ACCOUNT) -> dict[str, object]:
    resp = client.get("/api/health/status", query_string={"account_emails": account})
    assert resp.status_code == 200
    payload = resp.get_json()
    assert isinstance(payload, dict)
    return payload


def _telegram_component(payload: dict[str, object]) -> dict[str, object]:
    components = {row["name"]: row for row in payload["components"]}
    return components["Telegram"]


def _page_bodies(client, account: str = ACCOUNT) -> dict[str, str]:
    bodies: dict[str, str] = {}
    for path in ("/health", "/partial/health"):
        resp = client.get(path, query_string={"account_email": account})
        assert resp.status_code == 200
        bodies[path] = resp.get_data(as_text=True)
    return bodies


def test_health_status_counts_one_delivery_unknown_email(tmp_path: Path) -> None:
    db_path = _build_db(tmp_path)
    ts = _now_ts() - 3600
    _seed_delivery_unknown(db_path, email_id=UNKNOWN_EMAIL_ID, ts_utc=ts)
    app = _app(db_path)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        payload = _status_payload(client)

    assert payload["telegram_delivery_unknown_24h"] == 1
    assert _telegram_component(payload)["delivery_unknown_24h"] == 1


def test_health_page_shows_unconfirmed_delivery_count(tmp_path: Path) -> None:
    db_path = _build_db(tmp_path)
    _seed_delivery_unknown(db_path, email_id=UNKNOWN_EMAIL_ID, ts_utc=_now_ts() - 600)
    app = _app(db_path)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        bodies = _page_bodies(client)

    for path, body in bodies.items():
        assert COUNT_MARKER in body, path
        assert "Unconfirmed deliveries (24h): 1" in body, path


def test_repeated_unknown_events_for_one_email_count_once(tmp_path: Path) -> None:
    db_path = _build_db(tmp_path)
    ts = _now_ts() - 900
    _seed_delivery_unknown(db_path, email_id=UNKNOWN_EMAIL_ID, ts_utc=ts)
    # A second report for the same email (e.g. the claim check after a restart).
    _emit(
        db_path,
        event_type=EventType.TELEGRAM_FAILED,
        email_id=UNKNOWN_EMAIL_ID,
        ts_utc=ts + 60,
        payload={**_unknown_payload(ts + 60), "stage": "TG"},
    )
    app = _app(db_path)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        payload = _status_payload(client)

    assert payload["telegram_delivery_unknown_24h"] == 1


def test_count_ignores_old_delivered_plain_failures_and_other_accounts(
    tmp_path: Path,
) -> None:
    db_path = _build_db(tmp_path)
    now = _now_ts()
    # Outside the 24h window.
    _seed_delivery_unknown(db_path, email_id=101, ts_utc=now - 25 * 3600)
    # Unknown first, delivered after all: a delivery wins.
    _seed_delivery_unknown(db_path, email_id=102, ts_utc=now - 1800)
    _emit(
        db_path,
        event_type=EventType.TELEGRAM_DELIVERED,
        email_id=102,
        ts_utc=now - 1700,
        payload={"delivered": True, "occurred_at_utc": now - 1700, "mode": "html"},
    )
    # A definite failure is not an unconfirmed delivery.
    _seed_email(db_path, email_id=103)
    _emit(
        db_path,
        event_type=EventType.TELEGRAM_FAILED,
        email_id=103,
        ts_utc=now - 1200,
        payload={"delivered": False, "error": "chat not found"},
    )
    # Another account is outside the selected scope.
    _seed_delivery_unknown(
        db_path, email_id=104, ts_utc=now - 600, account=OTHER_ACCOUNT
    )
    app = _app(db_path)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        payload = _status_payload(client)
        bodies = _page_bodies(client)
        other_payload = _status_payload(client, account=OTHER_ACCOUNT)

    assert payload["telegram_delivery_unknown_24h"] == 0
    assert _telegram_component(payload)["delivery_unknown_24h"] == 0
    for path, body in bodies.items():
        assert COUNT_MARKER not in body, path
    assert other_payload["telegram_delivery_unknown_24h"] == 1


def test_delivery_unknown_surface_is_aggregate_only(tmp_path: Path) -> None:
    db_path = _build_db(tmp_path)
    _seed_delivery_unknown(db_path, email_id=UNKNOWN_EMAIL_ID, ts_utc=_now_ts() - 300)
    app = _app(db_path)

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        api_body = client.get(
            "/api/health/status", query_string={"account_emails": ACCOUNT}
        ).get_data(as_text=True)
        bodies = _page_bodies(client)

    for path, body in {"/api/health/status": api_body, **bodies}.items():
        assert SECRET_SUBJECT not in body, path
        assert SECRET_SENDER not in body, path
        assert str(UNKNOWN_EMAIL_ID) not in body, path


def test_unreadable_delivery_evidence_is_logged_and_shown(
    tmp_path: Path, caplog
) -> None:
    db_path = _build_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE events_v1")
    app = _app(db_path)

    with caplog.at_level(logging.WARNING):
        with app.test_client() as client:
            login_with_csrf(client, "pw")
            payload = _status_payload(client)
            bodies = _page_bodies(client)

    assert payload["telegram_delivery_unknown_24h"] is None
    assert _telegram_component(payload)["delivery_unknown_24h"] is None
    for path, body in bodies.items():
        assert COUNT_MARKER in body, path
        assert "Unconfirmed deliveries (24h): unavailable" in body, path
    assert any(
        record.getMessage() == "web_health_delivery_unknown_unavailable"
        for record in caplog.records
    )
