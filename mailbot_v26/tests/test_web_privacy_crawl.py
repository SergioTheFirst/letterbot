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


# Settings lane (S1): /accounts reads accounts.ini and runtime_health.json.
SETTINGS_LOGIN = "settings.owner@gmail.com"
SETTINGS_PASSWORD = "SETTINGS-PASSWORD-2W"
SETTINGS_TOKEN = "555666777:SETTINGS-TOKEN-ZXCVBNMASDFGHJKL"
SETTINGS_ERROR = "SETTINGS-LAST-ERROR-7M"
SETTINGS_MARKERS = (
    SETTINGS_LOGIN,
    SETTINGS_PASSWORD,
    SETTINGS_TOKEN,
    "SETTINGS-TOKEN",
    SETTINGS_ERROR,
)


def _seed_settings(tmp_path: Path) -> tuple[Path, Path]:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text("[ui]\nlocale = en\n", encoding="utf-8")
    (config_dir / "accounts.ini").write_text(
        "[work_mail]\n"
        f"login = {SETTINGS_LOGIN}\n"
        f"password = {SETTINGS_PASSWORD}\n"
        "host = imap.gmail.com\n"
        "port = 993\n"
        "use_ssl = true\n"
        "telegram_chat_id = 272250747\n"
        "[telegram]\n"
        f"bot_token = {SETTINGS_TOKEN}\n",
        encoding="utf-8",
    )
    runtime_path = tmp_path / "runtime_health.json"
    runtime_path.write_text(
        json.dumps(
            {
                "work_mail": {
                    "failure_bucket": "imap_auth",
                    "consecutive_failures": 1,
                    "last_error": SETTINGS_ERROR,
                    "last_error_class": "LoginError",
                    "last_alert_fingerprint": f"LoginError:{SETTINGS_ERROR}:h:993",
                }
            }
        ),
        encoding="utf-8",
    )
    return config_dir, runtime_path


@pytest.mark.parametrize("allow_pii", [False, True], ids=["pii_off", "pii_on"])
def test_no_route_shows_settings_secrets(tmp_path: Path, allow_pii: bool) -> None:
    db_path = _seed_db(tmp_path)
    config_dir, runtime_path = _seed_settings(tmp_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_pii=allow_pii,
        api_token=API_TOKEN,
        config_dir=config_dir,
    )
    app.config["SETTINGS_RUNTIME_HEALTH_PATH"] = runtime_path
    paths = _crawl_paths(app)
    assert "/accounts" in paths and "/api/accounts/status" in paths

    leaks: dict[str, list[str]] = {}
    with app.test_client() as client:
        accounts_page = client.get("/accounts").get_data(as_text=True)
        for path in paths:
            target = _with_pii(path) if allow_pii else path
            response = client.get(target, headers={"X-Api-Token": API_TOKEN})
            body = response.get_data(as_text=True)
            found = [
                marker for marker in (*MARKERS, *SETTINGS_MARKERS) if marker in body
            ]
            if found:
                leaks[path] = found

    # Not vacuous: the page rendered the mailbox row with the masked login.
    assert "s…@gmail.com" in accounts_page
    assert "Login rejected" in accounts_page
    assert leaks == {}
