import json
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


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


def _prepare_app(tmp_path: Path, *, attention_cost: float = 0.0):
    db_path = tmp_path / "attention.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    base_ts = 1000.0
    for idx in range(3):
        _emit_event(
            emitter,
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=base_ts + idx,
            account_id="primary@example.com",
            email_id=idx + 1,
            payload={
                "from_email": "alice@example.com",
                "subject": "Secret Subject",
                "body_chars": 800,
                "word_count": 160,
                "attachments_count": 1,
            },
        )
    for idx in range(2):
        _emit_event(
            emitter,
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=base_ts + 10 + idx,
            account_id="primary@example.com",
            email_id=10 + idx,
            payload={
                "from_email": "bob@example.com",
                "subject": "Hidden Subject",
                "body_chars": 400,
                "word_count": 80,
                "attachments_count": 0,
            },
        )
    _emit_event(
        emitter,
        event_type=EventType.ATTENTION_DEFERRED_FOR_DIGEST,
        ts_utc=base_ts + 20,
        account_id="primary@example.com",
        email_id=1,
        payload={},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=base_ts + 30,
        account_id="primary@example.com",
        entity_id="alice@example.com",
        email_id=1,
        payload={"trust_score": 0.75},
    )
    _emit_event(
        emitter,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        ts_utc=base_ts + 40,
        account_id="primary@example.com",
        entity_id="alice@example.com",
        email_id=2,
        payload={"health_score": 80.0},
    )
    _emit_event(
        emitter,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=base_ts + 50,
        account_id="secondary@example.com",
        email_id=99,
        payload={
            "from_email": "intruder@example.com",
            "subject": "PII",
            "body_chars": 300,
            "word_count": 60,
            "attachments_count": 0,
        },
    )
    return create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        attention_cost_per_hour=attention_cost,
    )


def test_attention_auth_required(tmp_path: Path) -> None:
    app = _prepare_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/attention")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")
        client.post("/login", data={"password": "pw"})
        response = client.get("/attention")
        assert response.status_code == 200


def test_attention_api_deterministic(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _prepare_app(tmp_path, attention_cost=120.0)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        query = {
            "account_email": "primary@example.com",
            "account_emails": "primary@example.com",
            "sort": "time",
        }
        first = client.get(
            "/api/v1/intelligence/attention_economics", query_string=query
        )
        second = client.get(
            "/api/v1/intelligence/attention_economics", query_string=query
        )
        assert first.status_code == 200
        assert second.status_code == 200
        assert first.get_json() == second.get_json()
        payload = first.get_json()
        assert payload["totals"]["message_count"] == 5
        assert payload["entities"][0]["estimated_cost"] > 0
        assert payload["sort"] == "time"


def test_attention_scope_isolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        primary_response = client.get(
            "/api/v1/intelligence/attention_economics",
            query_string={
                "account_email": "primary@example.com",
                "account_emails": "primary@example.com",
            },
        )
        secondary_response = client.get(
            "/api/v1/intelligence/attention_economics",
            query_string={
                "account_email": "secondary@example.com",
                "account_emails": "secondary@example.com",
            },
        )
        primary_entities = {item["entity_id"] for item in primary_response.get_json()["entities"]}
        secondary_entities = {item["entity_id"] for item in secondary_response.get_json()["entities"]}
        assert "intruder@example.com" not in primary_entities
        assert primary_entities
        assert secondary_entities == {"intruder@example.com"}


def test_attention_pii_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    forbidden_tokens = [
        "raw_body",
        "body_text",
        "html_text",
        "subject",
        "telegram_text",
        "rendered_message",
        "digest_text",
    ]
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        api_response = client.get(
            "/api/v1/intelligence/attention_economics",
            query_string={"account_email": "primary@example.com"},
        )
        assert api_response.status_code == 200
        api_text = json.dumps(api_response.get_json(), ensure_ascii=False)
        for token in forbidden_tokens:
            assert token not in api_text
        page_response = client.get(
            "/attention",
            query_string={"account_email": "primary@example.com"},
        )
        assert page_response.status_code == 200
        page_text = page_response.get_data(as_text=True)
        for token in forbidden_tokens:
            assert token not in page_text


def test_attention_ui_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        response = client.get(
            "/attention", query_string={"account_email": "primary@example.com"}
        )
        assert response.status_code == 200
        text = response.get_data(as_text=True)
        assert "attention-table" in text
