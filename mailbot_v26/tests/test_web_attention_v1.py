import csv
import io
import re
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
    email_id: int,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=None,
            email_id=email_id,
            payload=payload,
        )
    )


def _prepare_app(tmp_path: Path) -> object:
    db_path = tmp_path / "attention_v1.sqlite"
    KnowledgeDB(db_path)
    emitter = ContractEventEmitter(db_path)
    base_ts = 1000.0
    email_id = 1
    for sender, count in (
        ("alice@example.com", 3),
        ("bob@example.com", 3),
        ("charlie@example.com", 2),
    ):
        for idx in range(count):
            _emit_event(
                emitter,
                event_type=EventType.EMAIL_RECEIVED,
                ts_utc=base_ts + email_id + idx,
                account_id="primary@example.com",
                email_id=email_id,
                payload={
                    "from_email": sender,
                    "word_count": 200,
                    "body_chars": 1000,
                },
            )
            email_id += 1
    return create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        attention_cost_per_hour=120.0,
    )


def test_attention_sort_determinism(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    expected = ["alice@example.com", "bob@example.com", "charlie@example.com"]
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        for sort_mode in ("time", "cost", "count"):
            query = {"account_emails": "primary@example.com", "sort": sort_mode}
            first = client.get(
                "/api/v1/intelligence/attention_economics", query_string=query
            )
            second = client.get(
                "/api/v1/intelligence/attention_economics", query_string=query
            )
            assert first.status_code == 200
            assert second.status_code == 200
            assert first.get_json() == second.get_json()
            entities = first.get_json()["entities"]
            order = [item["entity_id"] for item in entities]
            assert order == expected


def test_attention_csv_matches_html_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        query = {"account_emails": "primary@example.com", "sort": "time"}
        html_response = client.get("/attention", query_string=query)
        csv_response = client.get("/attention.csv", query_string=query)
        assert html_response.status_code == 200
        assert csv_response.status_code == 200
        html_text = html_response.get_data(as_text=True)
        match = re.search(
            r"<table class=\"table compact fixed attention-table\">.*?<tbody>(.*?)</tbody>",
            html_text,
            re.S,
        )
        assert match is not None
        table_rows = match.group(1).count("<tr")
        csv_text = csv_response.get_data(as_text=True)
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        assert len(rows) - 1 == table_rows


def test_attention_outputs_avoid_banned_phrases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _prepare_app(tmp_path)
    monkeypatch.setattr(
        "mailbot_v26.storage.analytics.KnowledgeAnalytics._window_start_ts",
        lambda self, days: 0.0,
    )
    forbidden_phrases = [
        "no data",
        "nothing to show",
        "all quiet",
        "нет " + "данных",
    ]
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        for path in ("/attention", "/attention.csv"):
            response = client.get(
                path, query_string={"account_emails": "primary@example.com"}
            )
            assert response.status_code == 200
            body = response.get_data(as_text=True).lower()
            for phrase in forbidden_phrases:
                assert phrase not in body
