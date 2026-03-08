import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    entity_id: str | None = None,
    email_id: int | None = None,
    payload: dict[str, object] | None = None,
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=entity_id,
            email_id=email_id,
            payload=payload or {},
        )
    )


def _prepare_app(tmp_path: Path) -> object:
    db_path = tmp_path / "learning.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    base_ts = 1_000.0
    _emit_event(
        emitter,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=base_ts,
        account_id="primary@example.com",
        email_id=1,
        payload={"subject": "Secret", "from_email": "alice@example.com"},
    )
    _emit_event(
        emitter,
        event_type=EventType.PRIORITY_CORRECTION_RECORDED,
        ts_utc=base_ts + 50,
        account_id="primary@example.com",
        email_id=1,
        payload={
            "old_priority": "🔵",
            "new_priority": "🔴",
            "source": "telegram_inbound",
            "sender_email": "alice@example.com",
            "account_email": "primary@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.SURPRISE_DETECTED,
        ts_utc=base_ts + 55,
        account_id="primary@example.com",
        email_id=1,
        payload={"old_priority": "🔵", "new_priority": "🔴", "delta": 2},
    )
    _emit_event(
        emitter,
        event_type=EventType.DELIVERY_POLICY_APPLIED,
        ts_utc=base_ts + 60,
        account_id="primary@example.com",
        email_id=1,
        payload={
            "mode": "defer",
            "reason_codes": ["attention_debt"],
            "sources": "body text should be hidden",
            "priority": "🔴",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.ATTENTION_DEBT_UPDATED,
        ts_utc=base_ts + 70,
        account_id="primary@example.com",
        email_id=1,
        payload={
            "bucket": "high",
            "attention_debt": 90,
            "from_email": "pii@example.com",
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.CALIBRATION_PROPOSALS_GENERATED,
        ts_utc=base_ts + 80,
        account_id="primary@example.com",
        payload={
            "week_key": "2026-W01",
            "proposals_count": 2,
            "top_labels": ["label-a"],
        },
    )
    _emit_event(
        emitter,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=base_ts,
        account_id="secondary@example.com",
        email_id=99,
        payload={"subject": "Leak", "from_email": "intruder@example.com"},
    )
    _emit_event(
        emitter,
        event_type=EventType.DELIVERY_POLICY_APPLIED,
        ts_utc=base_ts + 10,
        account_id="secondary@example.com",
        email_id=99,
        payload={"mode": "immediate", "reason_codes": ["urgent"]},
    )
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _freeze_now(monkeypatch: pytest.MonkeyPatch, *, ts: float) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromtimestamp(ts, tz=tz or timezone.utc)

    monkeypatch.setattr("mailbot_v26.web_observability.app.datetime", FrozenDateTime)


def test_learning_auth_required(tmp_path: Path) -> None:
    app = _prepare_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/learning")
        assert response.status_code == 302
        api_response = client.get("/api/v1/intelligence/learning_summary")
        assert api_response.status_code == 302


def test_learning_api_deterministic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    _freeze_now(monkeypatch, ts=2_000.0)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        query = {
            "account_email": "primary@example.com",
            "account_emails": "primary@example.com",
            "window": "30",
            "limit": "50",
        }
        first = client.get("/api/v1/intelligence/learning_summary", query_string=query)
        second = client.get("/api/v1/intelligence/learning_summary", query_string=query)
        assert first.status_code == 200
        assert second.status_code == 200
        assert first.get_json() == second.get_json()
        payload = first.get_json()
        assert payload["corrections"] == 1
        assert payload["surprises"] == 1


def test_learning_scope_isolation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    _freeze_now(monkeypatch, ts=2_000.0)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        primary = client.get(
            "/api/v1/intelligence/learning_summary",
            query_string={
                "account_email": "primary@example.com",
                "account_emails": "primary@example.com",
            },
        )
        secondary = client.get(
            "/api/v1/intelligence/learning_summary",
            query_string={
                "account_email": "secondary@example.com",
                "account_emails": "secondary@example.com",
            },
        )
        assert primary.status_code == 200
        assert secondary.status_code == 200
        assert primary.get_json()["surprises"] == 1
        assert secondary.get_json()["surprises"] == 0


def test_learning_default_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    _freeze_now(monkeypatch, ts=2_000.0)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get(
            "/api/v1/intelligence/learning_timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["window_days"] == 30
        page_response = client.get(
            "/learning", query_string={"account_email": "primary@example.com"}
        )
        assert page_response.status_code == 200
        assert "Learning timeline" in page_response.get_data(as_text=True)


def test_learning_pii_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _prepare_app(tmp_path)
    _freeze_now(monkeypatch, ts=2_000.0)
    forbidden_tokens = ["subject", "body", "raw", "from_email", "sender_email"]
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        api_response = client.get(
            "/api/v1/intelligence/learning_timeline",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_response.status_code == 200
        api_payload = api_response.get_json()
        for item in api_payload.get("items", []):
            payload_text = json.dumps(item.get("payload") or {}, ensure_ascii=False)
            for token in forbidden_tokens:
                assert token not in payload_text
            assert "@" not in payload_text
        page_response = client.get(
            "/learning",
            query_string={"account_email": "primary@example.com"},
        )
        assert page_response.status_code == 200
        page_text = page_response.get_data(as_text=True)
        forbidden_page_tokens = [
            "body text should be hidden",
            "alice@example.com",
            "pii@example.com",
            "sender_email",
        ]
        for token in forbidden_page_tokens:
            assert token not in page_text
