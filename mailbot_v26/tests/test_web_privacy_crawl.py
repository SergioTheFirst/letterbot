"""Privacy crawl: the web cockpit never shows raw email subjects or senders.

CONSTITUTION: "Web = read-only Intelligence Hub ... never show raw email
text/subjects/senders in web." The crawl seeds one email whose subject and
sender are unique markers, requests every parameterless GET route plus the
per-email detail routes, and asserts that neither marker appears anywhere in
any response body - with the PII toggle off and on.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.events.contract import EventType, EventV1, fingerprint
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

SECRET_SUBJECT = "SECRET-SUBJECT-7Q"
SECRET_SENDER = "secret.sender@example.com"
# issuer_profile falls back to the full address for generic mail domains.
SECRET_ISSUER_SENDER = "secret.issuer@gmail.com"
MARKERS = (SECRET_SUBJECT, SECRET_SENDER, SECRET_ISSUER_SENDER)
ACCOUNT = "acct@example.com"
EMAIL_ID = 7
ISSUER_EMAIL_ID = 8
API_TOKEN = "crawl-token"

# Routes that still leak and cannot be fixed inside the web lane.
# path -> reason. Keep this empty unless the leak lives in a module the web
# lane does not own.
KNOWN_VIOLATORS: dict[str, str] = {}


def _insert_event(
    db_path: Path,
    *,
    event_type: EventType,
    ts_utc: float,
    payload: dict[str, object],
    email_id: int = EMAIL_ID,
) -> None:
    event = EventV1(
        event_type=event_type,
        ts_utc=ts_utc,
        account_id=ACCOUNT,
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


def _seed_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "privacy.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, received_at,
                                action_line, body_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                EMAIL_ID,
                ACCOUNT,
                SECRET_SENDER,
                SECRET_SUBJECT,
                now.isoformat(),
                "Check invoice",
                "Short summary",
            ),
        )
    _insert_event(
        db_path,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=now.timestamp(),
        payload={"from_email": SECRET_SENDER, "subject": SECRET_SUBJECT},
    )
    # No issuer_label on purpose: the display must not fall back to the raw sender.
    _insert_event(
        db_path,
        event_type=EventType.MESSAGE_INTERPRETATION,
        ts_utc=now.timestamp(),
        payload={
            "sender_email": SECRET_SENDER,
            "doc_kind": "invoice",
            "amount": 12500,
            "due_date": "2026-04-15",
            "priority": "yellow",
            "action": "Проверить",
            "confidence": 0.86,
            "context": "NEW_MESSAGE",
            "document_id": "INV-007",
        },
    )
    # Second email: the issuer label itself is the raw address.
    _insert_event(
        db_path,
        event_type=EventType.EMAIL_RECEIVED,
        ts_utc=now.timestamp() - 60,
        payload={"from_email": SECRET_ISSUER_SENDER, "subject": SECRET_SUBJECT},
        email_id=ISSUER_EMAIL_ID,
    )
    _insert_event(
        db_path,
        event_type=EventType.MESSAGE_INTERPRETATION,
        ts_utc=now.timestamp() - 60,
        payload={
            "sender_email": SECRET_ISSUER_SENDER,
            "issuer_label": SECRET_ISSUER_SENDER,
            "issuer_key": f"email:{SECRET_ISSUER_SENDER}",
            "doc_kind": "invoice",
            "priority": "yellow",
            "confidence": 0.7,
            "context": "NEW_MESSAGE",
        },
        email_id=ISSUER_EMAIL_ID,
    )
    return db_path


def _crawl_paths(app) -> list[str]:
    paths: set[str] = set()
    for rule in app.url_map.iter_rules():
        if "GET" not in (rule.methods or set()):
            continue
        if rule.arguments:
            continue
        if rule.endpoint in {"static", "login"}:
            continue
        paths.add(rule.rule)
    paths.update(
        {
            f"/api/archive?account_emails={ACCOUNT}",
        }
    )
    for email_id in (EMAIL_ID, ISSUER_EMAIL_ID):
        paths.update(
            {
                f"/email/{email_id}",
                f"/api/archive/{email_id}/detail",
                f"/archive?message_id={email_id}",
            }
        )
    return sorted(paths)


def _with_pii(path: str) -> str:
    return f"{path}{'&' if '?' in path else '?'}pii=1"


@pytest.mark.parametrize("allow_pii", [False, True], ids=["pii_off", "pii_on"])
def test_no_route_shows_raw_subject_or_sender(tmp_path: Path, allow_pii: bool) -> None:
    db_path = _seed_db(tmp_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_pii=allow_pii,
        api_token=API_TOKEN,
    )
    paths = _crawl_paths(app)
    assert "/archive" in paths and "/api/archive" in paths

    leaks: dict[str, list[str]] = {}
    with app.test_client() as client:
        for path in paths:
            target = _with_pii(path) if allow_pii else path
            response = client.get(target, headers={"X-Api-Token": API_TOKEN})
            body = response.get_data(as_text=True)
            found = [marker for marker in MARKERS if marker in body]
            if found:
                leaks[path] = found

    unexpected = {path: found for path, found in leaks.items() if path not in KNOWN_VIOLATORS}
    assert unexpected == {}
    stale = sorted(path for path in KNOWN_VIOLATORS if path not in leaks)
    assert stale == [], f"KNOWN_VIOLATORS entries no longer leak: {stale}"


def test_dashboard_top_issuers_mask_generic_sender(tmp_path: Path) -> None:
    db_path = _seed_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        first = client.get("/api/dashboard").get_json()["top_issuers"]
        second = client.get("/api/dashboard").get_json()["top_issuers"]

    generic = [item for item in first if item["issuer_label"] == "s…@gmail.com"]
    assert len(generic) == 1, first
    issuer_key = str(generic[0]["issuer_key"])
    assert "@" not in issuer_key
    assert SECRET_ISSUER_SENDER not in issuer_key
    assert issuer_key.startswith("email-sha256:")
    # Stable across requests, so the key still works as a client-side id.
    assert [item["issuer_key"] for item in first] == [
        item["issuer_key"] for item in second
    ]


def test_archive_sender_filter_cannot_probe_raw_address(tmp_path: Path) -> None:
    db_path = _seed_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        visible = client.get("/api/archive", query_string={"account_emails": ACCOUNT})
        assert visible.get_json()["total"] == 2
        for raw_address in (SECRET_SENDER, SECRET_ISSUER_SENDER):
            probe = client.get(
                "/api/archive",
                query_string={"account_emails": ACCOUNT, "sender": raw_address},
            )
            assert probe.get_json()["total"] == 0, raw_address


def test_archive_row_has_no_raw_subject_or_sender_fields(tmp_path: Path) -> None:
    db_path = _seed_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret", allow_pii=True)
    with app.test_client() as client:
        payload = client.get(
            "/api/archive", query_string={"account_emails": ACCOUNT, "pii": "1"}
        ).get_json()
    items = {item["message_id"]: item for item in payload["items"]}
    assert set(items) == {EMAIL_ID, ISSUER_EMAIL_ID}
    for item in items.values():
        assert "subject" not in item
        assert "subject_full" not in item
        assert "sender_email" not in item
        assert item["issuer_display"] == item["sender_display"]
        for marker in MARKERS:
            assert marker not in str(item)
    # Masked, still useful: first letter and domain.
    assert items[EMAIL_ID]["sender_display"] == "s…@example.com"
    assert items[ISSUER_EMAIL_ID]["sender_display"] == "s…@gmail.com"


# --- Scoped crawl: parameterized routes with the seeded account (P3b D1/C1) ---
# The parameterless crawl above never selects an account on routes that need
# one, so it misses everything that only renders for a scope. This crawl adds
# the account parameters to every route, plus a resolved sender entity (the
# runtime keys contacts by entity UUID) and per-entity events.

SECRET_SENDER_NAME = "Secret Sender Name 7Q"
CONTACT_ENTITY_ID = "3f2c1a9e-5b7d-4c11-9a0e-7d2b8c4e6f10"
SCOPED_MARKERS = (*MARKERS, SECRET_SENDER_NAME)
SCOPE_QUERY = f"account_emails={ACCOUNT}&account_email={ACCOUNT}"

# Same contract as KNOWN_VIOLATORS: path -> reason, only for leaks that live
# in modules the web lane does not own.
SCOPED_KNOWN_VIOLATORS: dict[str, str] = {}


def _seed_scoped_db(tmp_path: Path) -> Path:
    # Local import keeps this file's existing header untouched (append-only).
    from mailbot_v26.observability.processing_span import ProcessingSpanRecorder

    db_path = _seed_db(tmp_path)
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO entities (id, type, name, normalized_name, first_seen,
                                  last_seen, metadata)
            VALUES (?, 'person', ?, ?, ?, ?, ?)
            """,
            (
                CONTACT_ENTITY_ID,
                SECRET_SENDER_NAME,
                SECRET_SENDER_NAME.lower(),
                now.isoformat(),
                now.isoformat(),
                json.dumps(
                    {"from_email": SECRET_SENDER, "from_name": SECRET_SENDER_NAME}
                ),
            ),
        )
        entity_events = (
            (
                "email_received",
                {
                    "from_email": SECRET_SENDER,
                    "from_name": SECRET_SENDER_NAME,
                    "subject": SECRET_SUBJECT,
                },
            ),
            ("telegram_delivered", {"delivery_mode": "IMMEDIATE"}),
            ("trust_score_updated", {"trust_score": 0.8}),
        )
        for index, (event_type, payload) in enumerate(entity_events):
            payload_json = json.dumps(payload, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO events_v1 (
                    event_type, ts_utc, ts, account_id, entity_id, email_id,
                    payload, payload_json, schema_version, fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    event_type,
                    now.timestamp() + index,
                    now.isoformat(),
                    ACCOUNT,
                    CONTACT_ENTITY_ID,
                    EMAIL_ID,
                    payload_json,
                    payload_json,
                    f"scoped-crawl:{event_type}",
                ),
            )
        conn.execute(
            """
            INSERT INTO processing_spans (
                span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id,
                email_id, stage_durations_json, llm_provider, llm_model,
                llm_latency_ms, outcome, error_code, health_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "span-scoped-crawl",
                now.timestamp() - 2,
                now.timestamp(),
                2000,
                ACCOUNT,
                EMAIL_ID,
                json.dumps({"parse": 100, "llm": 1500}),
                "gigachat",
                "giga-pro",
                1500,
                "error",
                "LLM_TIMEOUT",
                "",
            ),
        )
        conn.execute(
            """
            INSERT INTO commitments (email_row_id, source, commitment_text,
                                     deadline_iso, status, confidence, created_at)
            VALUES (?, 'heuristic', 'Pay the invoice', '2026-11-01', 'pending', 0.9, ?)
            """,
            (EMAIL_ID, now.strftime("%Y-%m-%d %H:%M:%S")),
        )
    return db_path


def _scoped_crawl_paths(app) -> list[str]:
    paths: set[str] = set()
    for rule in app.url_map.iter_rules():
        if "GET" not in (rule.methods or set()):
            continue
        if rule.arguments or rule.endpoint in {"static", "login"}:
            continue
        paths.add(f"{rule.rule}?{SCOPE_QUERY}")
    contact = f"contact_id=contact:{CONTACT_ENTITY_ID}"
    paths.update(
        {
            f"/?{SCOPE_QUERY}&mode=engineer",
            f"/health?{SCOPE_QUERY}&mode=engineer",
            f"/relationships/contact?{SCOPE_QUERY}&{contact}",
            f"/api/v1/relationships/contact?{SCOPE_QUERY}&{contact}",
        }
    )
    for email_id in (EMAIL_ID, ISSUER_EMAIL_ID):
        paths.update(
            {
                f"/email/{email_id}?{SCOPE_QUERY}&mode=engineer",
                f"/api/archive/{email_id}/detail?{SCOPE_QUERY}",
                f"/archive?{SCOPE_QUERY}&message_id={email_id}",
            }
        )
    return sorted(paths)


@pytest.mark.parametrize("allow_pii", [False, True], ids=["pii_off", "pii_on"])
def test_no_scoped_route_shows_raw_subject_or_sender(
    tmp_path: Path, allow_pii: bool
) -> None:
    db_path = _seed_scoped_db(tmp_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_pii=allow_pii,
        api_token=API_TOKEN,
    )
    paths = _scoped_crawl_paths(app)
    assert f"/api/v1/intelligence/attention_economics?{SCOPE_QUERY}" in paths

    leaks: dict[str, list[str]] = {}
    statuses: dict[str, int] = {}
    with app.test_client() as client:
        for path in paths:
            target = _with_pii(path) if allow_pii else path
            response = client.get(target, headers={"X-Api-Token": API_TOKEN})
            statuses[path] = response.status_code
            body = response.get_data(as_text=True)
            found = [marker for marker in SCOPED_MARKERS if marker in body]
            if found:
                leaks[path] = found

    # The scoped routes must really render (a 400/500 would hide a leak).
    assert statuses[f"/api/v1/intelligence/attention_economics?{SCOPE_QUERY}"] == 200
    assert statuses[f"/api/v1/relationships/graph?{SCOPE_QUERY}"] == 200
    contact_api = (
        f"/api/v1/relationships/contact?{SCOPE_QUERY}"
        f"&contact_id=contact:{CONTACT_ENTITY_ID}"
    )
    assert statuses[contact_api] == 200
    unexpected = {
        path: found for path, found in leaks.items() if path not in SCOPED_KNOWN_VIOLATORS
    }
    assert unexpected == {}
    stale = sorted(path for path in SCOPED_KNOWN_VIOLATORS if path not in leaks)
    assert stale == [], f"SCOPED_KNOWN_VIOLATORS entries no longer leak: {stale}"


def test_attention_api_hashes_sender_entity_ids(tmp_path: Path) -> None:
    db_path = _seed_db(tmp_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    query = {"account_emails": ACCOUNT}
    with app.test_client() as client:
        first = client.get(
            "/api/v1/intelligence/attention_economics", query_string=query
        ).get_json()
        second = client.get(
            "/api/v1/intelligence/attention_economics", query_string=query
        ).get_json()

    entity_ids = [str(item["entity_id"]) for item in first["entities"]]
    assert len(entity_ids) == 2
    for entity_id in entity_ids:
        assert entity_id.startswith("email-sha256:")
        assert "@" not in entity_id
    # Stable across requests, so the key still works as a client-side id.
    assert entity_ids == [str(item["entity_id"]) for item in second["entities"]]
    assert {item["entity_label"] for item in first["entities"]} == {
        "s…@example.com",
        "s…@gmail.com",
    }
    for marker in MARKERS:
        assert marker not in json.dumps(first, ensure_ascii=False)


# --- Unusual sender shapes (P3b D1/C1 fix round) ---
# The markers above are ASCII dotted addresses, and jsonify escapes non-ASCII
# as \uXXXX, so a plain substring check never sees an IDN sender. These shapes
# defeat an ASCII e-mail pattern: IDN local part or domain, a dotless host, and
# a from_email that is only a display name.

UNUSUAL_SENDERS = (
    "root@localhost",
    "ivan@пример.com",
    "иван@example.com",
    "иван.петров@почта.рф",
    "ops.bot@mailhost",
    "Ivan Petrov Secret",
    # Generic mail domain: issuer_profile keys and labels it by the address.
    "иван@mail.ru",
)
UNUSUAL_EMAIL_ID_BASE = 100


def _raw_forms(value: str) -> set[str]:
    """The value (and its lower-cased key) as HTML shows it and jsonify escapes it."""
    forms: set[str] = set()
    for variant in (value, value.lower()):
        forms.add(variant)
        forms.add(json.dumps(variant)[1:-1])
    return forms


def _leaked_unusual_senders(body: str) -> list[str]:
    return [
        sender
        for sender in UNUSUAL_SENDERS
        if any(form in body for form in _raw_forms(sender))
    ]


def _seed_unusual_senders(
    db_path: Path, senders: tuple[str, ...] = UNUSUAL_SENDERS
) -> list[int]:
    """One received email per sender, interpreted the way the pipeline does."""
    # Local import keeps this file's existing header untouched (append-only).
    from mailbot_v26.domain.issuer_profile import build_issuer_profile

    now = datetime.now(timezone.utc)
    email_ids: list[int] = []
    for index, sender in enumerate(senders):
        email_id = UNUSUAL_EMAIL_ID_BASE + index
        ts_utc = now.timestamp() - 120 - index
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO emails (id, account_email, from_email, subject,
                                    received_at, action_line, body_summary)
                VALUES (?, ?, ?, 'Invoice', ?, 'Check invoice', 'Short summary')
                """,
                (email_id, ACCOUNT, sender, now.isoformat()),
            )
        _insert_event(
            db_path,
            event_type=EventType.EMAIL_RECEIVED,
            ts_utc=ts_utc,
            payload={"from_email": sender},
            email_id=email_id,
        )
        profile = build_issuer_profile(sender_email=sender)
        assert profile is not None
        _insert_event(
            db_path,
            event_type=EventType.MESSAGE_INTERPRETATION,
            ts_utc=ts_utc,
            payload={
                "sender_email": sender,
                "issuer_key": profile.issuer_key,
                "issuer_label": profile.issuer_label,
                "doc_kind": "invoice",
                "priority": "yellow",
                "confidence": 0.7,
                "context": "NEW_MESSAGE",
            },
            email_id=email_id,
        )
        email_ids.append(email_id)
    return email_ids


def test_attention_api_hashes_unusual_sender_ids(tmp_path: Path) -> None:
    db_path = _seed_db(tmp_path)
    _seed_unusual_senders(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        response = client.get(
            "/api/v1/intelligence/attention_economics",
            query_string={"account_emails": ACCOUNT},
        )

    assert response.status_code == 200
    entities = response.get_json()["entities"]
    # The two senders of _seed_db plus one row per unusual sender.
    assert len(entities) == 2 + len(UNUSUAL_SENDERS)
    raw_keys = {SECRET_SENDER, SECRET_ISSUER_SENDER} | {
        sender.lower() for sender in UNUSUAL_SENDERS
    }
    entity_ids = [str(item["entity_id"]) for item in entities]
    for entity_id in entity_ids:
        assert entity_id.startswith("email-sha256:"), entity_id
        assert entity_id not in raw_keys
    # Still one distinct id per sender, so it keeps working as a client-side id.
    assert len(set(entity_ids)) == len(entity_ids)
    assert _leaked_unusual_senders(response.get_data(as_text=True)) == []


@pytest.mark.parametrize("allow_pii", [False, True], ids=["pii_off", "pii_on"])
def test_no_scoped_route_shows_unusual_sender_shapes(
    tmp_path: Path, allow_pii: bool
) -> None:
    db_path = _seed_scoped_db(tmp_path)
    email_ids = _seed_unusual_senders(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_pii=allow_pii,
        api_token=API_TOKEN,
    )
    paths = set(_scoped_crawl_paths(app))
    for email_id in email_ids:
        paths.update(
            {
                f"/email/{email_id}?{SCOPE_QUERY}&mode=engineer",
                f"/api/archive/{email_id}/detail?{SCOPE_QUERY}",
                f"/archive?{SCOPE_QUERY}&message_id={email_id}",
            }
        )

    leaks: dict[str, list[str]] = {}
    statuses: dict[str, int] = {}
    with app.test_client() as client:
        for path in sorted(paths):
            target = _with_pii(path) if allow_pii else path
            response = client.get(target, headers={"X-Api-Token": API_TOKEN})
            statuses[path] = response.status_code
            found = _leaked_unusual_senders(response.get_data(as_text=True))
            if found:
                leaks[path] = found

    # The routes that show senders must really render (an error hides a leak).
    for path in (
        f"/?{SCOPE_QUERY}",
        f"/api/dashboard?{SCOPE_QUERY}",
        f"/archive?{SCOPE_QUERY}",
        f"/api/archive?{SCOPE_QUERY}",
        f"/api/archive/{email_ids[-1]}/detail?{SCOPE_QUERY}",
        f"/email/{email_ids[-1]}?{SCOPE_QUERY}&mode=engineer",
        f"/api/v1/intelligence/attention_economics?{SCOPE_QUERY}",
    ):
        assert statuses[path] == 200, path
    assert leaks == {}


def test_sender_keyed_issuers_show_masked_labels(tmp_path: Path) -> None:
    # issuer_profile keys these two by the raw sender string: a generic mail
    # domain with an IDN local part, and a sender that has no address at all.
    db_path = tmp_path / "issuers.sqlite"
    KnowledgeDB(db_path)
    email_ids = _seed_unusual_senders(
        db_path, senders=("иван@mail.ru", "Ivan Petrov Secret")
    )
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    scope = {"account_emails": ACCOUNT}
    with app.test_client() as client:
        dashboard = client.get("/api/dashboard", query_string=scope).get_json()
        archive = client.get("/api/archive", query_string=scope).get_json()
        details = [
            client.get(
                f"/api/archive/{email_id}/detail", query_string=scope
            ).get_json()
            for email_id in email_ids
        ]

    expected = ["Sender hidden", "и…@mail.ru"]
    issuers = dashboard["top_issuers"]
    assert sorted(item["issuer_label"] for item in issuers) == expected
    for item in issuers:
        assert str(item["issuer_key"]).startswith("email-sha256:")
    processed = dashboard["processed_table"]["rows"]
    assert sorted(row["sender_display"] for row in processed) == expected
    assert sorted(item["sender_display"] for item in archive["items"]) == expected
    assert sorted(item["issuer_display"] for item in archive["items"]) == expected
    assert [detail["issuer_display"] for detail in details] == [
        "и…@mail.ru",
        "Sender hidden",
    ]


def test_web_address_masking_covers_unusual_shapes() -> None:
    from mailbot_v26.web_observability import app as web_app

    for address in (sender for sender in UNUSUAL_SENDERS if "@" in sender):
        local, _, domain = address.partition("@")
        line = f"Reply to {address} today"
        assert web_app._mask_emails_in_text(line) == (
            f"Reply to {local[0]}…@{domain} today"
        )
        assert web_app._scrub_pii_line(line) == "Reply to [redacted] today"
    # Labels without an address, or with only a handle, stay as they are.
    for label in ("ООО Ромашка", "vendor.example", "Support (@support)"):
        assert web_app._mask_emails_in_text(label) == label
        assert web_app._scrub_pii_line(label) == label
