"""Relationships count emails, label contacts readably and speak the UI language
(P3b D1/C9).

The graph counted every event carrying an email id as an "email", showed
the entity UUID as the contact name and labelled the user node "Вы" in the
English interface.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"
KNOWN_ENTITY = "5d1f4c3a-2b6e-4e8f-9a17-0c3b2d4e5f61"
UNKNOWN_ENTITY = "9e8d7c6b-5a49-4382-a1b0-f9e8d7c6b5a4"
SENDER = "jane.doe@acme.example"
SENDER_NAME = "Jane Doe"
SCOPE = {"account_email": ACCOUNT, "account_emails": ACCOUNT}


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    entity_id: str,
    email_id: int,
    payload: dict[str, object],
    ts_utc: float,
) -> None:
    payload_json = json.dumps(payload)
    conn.execute(
        """
        INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id,
                               email_id, payload, payload_json, schema_version,
                               fingerprint)
        VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            event_type,
            ts_utc,
            ACCOUNT,
            entity_id,
            email_id,
            payload_json,
            payload_json,
            f"{event_type}:{entity_id}:{email_id}",
        ),
    )


def _seed(tmp_path: Path) -> Path:
    db_path = tmp_path / "relationships-labels.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    ts = now.timestamp()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO entities (id, type, name, normalized_name, first_seen,
                                  last_seen, metadata)
            VALUES (?, 'person', ?, ?, ?, ?, ?)
            """,
            (
                KNOWN_ENTITY,
                SENDER_NAME,
                SENDER_NAME.lower(),
                now.isoformat(),
                now.isoformat(),
                json.dumps({"from_email": SENDER, "from_name": SENDER_NAME}),
            ),
        )
        # Email 1 produced three events, email 2 one: two emails in total.
        for offset, event_type in enumerate(
            ("email_received", "telegram_delivered", "trust_score_updated")
        ):
            _insert_event(
                conn,
                event_type=event_type,
                entity_id=KNOWN_ENTITY,
                email_id=1,
                payload={"trust_score": 0.7} if "trust" in event_type else {},
                ts_utc=ts - 100 + offset,
            )
        _insert_event(
            conn,
            event_type="email_received",
            entity_id=KNOWN_ENTITY,
            email_id=2,
            payload={},
            ts_utc=ts - 50,
        )
        _insert_event(
            conn,
            event_type="email_received",
            entity_id=UNKNOWN_ENTITY,
            email_id=3,
            payload={},
            ts_utc=ts - 40,
        )
        conn.commit()
    return db_path


def _ru_config(tmp_path: Path) -> Path:
    config_dir = tmp_path / "config-ru"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text("[ui]\nlocale = ru\n", encoding="utf-8")
    return config_dir


def _nodes(graph: dict[str, object]) -> dict[str, dict[str, object]]:
    return {str(node["id"]): node for node in graph["nodes"]}  # type: ignore[index]


def test_graph_counts_emails_not_events(tmp_path: Path) -> None:
    app = create_app(db_path=_seed(tmp_path), password="pw", secret_key="secret")
    with app.test_client() as client:
        graph = client.get("/api/v1/relationships/graph", query_string=SCOPE).get_json()
        detail = client.get(
            "/api/v1/relationships/contact",
            query_string={**SCOPE, "contact_id": f"contact:{KNOWN_ENTITY}"},
        ).get_json()

    node = _nodes(graph)[f"contact:{KNOWN_ENTITY}"]
    assert node["emails_total"] == 2
    edge = next(e for e in graph["edges"] if e["target"] == f"contact:{KNOWN_ENTITY}")
    assert edge["weight"] == 2
    assert detail["contact"]["emails_total"] == 2


def test_contacts_get_a_masked_address_label_not_a_uuid(tmp_path: Path) -> None:
    app = create_app(db_path=_seed(tmp_path), password="pw", secret_key="secret")
    with app.test_client() as client:
        response = client.get("/api/v1/relationships/graph", query_string=SCOPE)
        detail = client.get(
            "/api/v1/relationships/contact",
            query_string={**SCOPE, "contact_id": f"contact:{KNOWN_ENTITY}"},
        ).get_json()
        page = client.get("/relationships", query_string=SCOPE).get_data(as_text=True)

    nodes = _nodes(response.get_json())
    known = nodes[f"contact:{KNOWN_ENTITY}"]
    assert known["label"] == "j…@acme.example"
    assert known["domain"] == "acme.example"
    assert detail["contact"]["label"] == "j…@acme.example"
    unknown = nodes[f"contact:{UNKNOWN_ENTITY}"]
    assert str(unknown["label"]).startswith("contact-")
    assert UNKNOWN_ENTITY not in str(unknown["label"])
    for body in (response.get_data(as_text=True), json.dumps(detail), page):
        assert SENDER not in body
        assert SENDER_NAME not in body


def test_user_node_follows_the_interface_language(tmp_path: Path) -> None:
    db_path = _seed(tmp_path)
    english = create_app(db_path=db_path, password="pw", secret_key="secret")
    russian = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        config_dir=_ru_config(tmp_path),
    )
    with english.test_client() as client:
        en_graph = client.get("/api/v1/relationships/graph", query_string=SCOPE).get_json()
    with russian.test_client() as client:
        ru_graph = client.get("/api/v1/relationships/graph", query_string=SCOPE).get_json()

    assert _nodes(en_graph)["user:me"]["label"] == "You"
    assert _nodes(ru_graph)["user:me"]["label"] == "Вы"
