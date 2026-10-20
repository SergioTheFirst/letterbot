"""Home IMAP status is real and recovers (P3b D1/C4).

The IMAP badge on the home page was hard-coded UNKNOWN, the refresh script
updated an element id that does not exist, and a single dead-lettered UID
kept IMAP "degraded" forever because the dead-letter count was all-time.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"


def _insert_imap_event(
    conn: sqlite3.Connection, *, subtype: str, when: datetime, key: str
) -> None:
    payload = json.dumps({"subtype": subtype, "detail": subtype})
    conn.execute(
        """
        INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id,
                               email_id, payload, payload_json, schema_version,
                               fingerprint)
        VALUES ('imap_health_v1', ?, ?, ?, NULL, NULL, ?, ?, 1, ?)
        """,
        (when.timestamp(), when.isoformat(), ACCOUNT, payload, payload, key),
    )


def _app_with_imap_events(tmp_path: Path, events: list[tuple[str, timedelta]]):
    db_path = tmp_path / "imap-status.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO emails (account_email, from_email, subject, received_at) "
            "VALUES (?, 'a@example.com', 's', ?)",
            (ACCOUNT, now.isoformat()),
        )
        for index, (subtype, age) in enumerate(events):
            _insert_imap_event(conn, subtype=subtype, when=now - age, key=f"imap-{index}")
        conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _imap_badge(body: str) -> tuple[str, str]:
    match = re.search(
        r'<span class="badge ([a-z]+)" id="health-imap-status">([^<]*)</span>', body
    )
    assert match is not None, "IMAP badge missing"
    return match.group(1), match.group(2)


def test_home_imap_badge_shows_the_recorded_imap_status(tmp_path: Path) -> None:
    app = _app_with_imap_events(tmp_path, [("success", timedelta(seconds=30))])
    with app.test_client() as client:
        body = client.get("/", query_string={"account_emails": ACCOUNT}).get_data(
            as_text=True
        )
        runtime = client.get(
            "/api/dashboard", query_string={"account_emails": ACCOUNT}
        ).get_json()["runtime"]

    assert _imap_badge(body) == ("success", "OK")
    assert runtime["imap_status_label"] == "OK"
    assert runtime["imap_status_class"] == "success"


def test_home_imap_badge_without_evidence_stays_unknown(tmp_path: Path) -> None:
    app = _app_with_imap_events(tmp_path, [])
    with app.test_client() as client:
        body = client.get("/", query_string={"account_emails": ACCOUNT}).get_data(
            as_text=True
        )

    assert _imap_badge(body) == ("muted", "UNKNOWN")


def test_home_refresh_script_only_targets_existing_ids(tmp_path: Path) -> None:
    app = _app_with_imap_events(tmp_path, [("success", timedelta(seconds=30))])
    with app.test_client() as client:
        body = client.get("/", query_string={"account_emails": ACCOUNT}).get_data(
            as_text=True
        )

    script_ids = set(
        re.findall(r"(?:setText|setBadge|renderList|byId)\('([A-Za-z0-9_-]+)'", body)
    )
    assert "health-imap-status" in script_ids
    assert "health-pipeline-status" in script_ids
    missing = sorted(i for i in script_ids if f'id="{i}"' not in body)
    assert missing == []


def test_an_old_dead_letter_no_longer_degrades_imap(tmp_path: Path) -> None:
    app = _app_with_imap_events(
        tmp_path,
        [("success", timedelta(seconds=30)), ("dead_letter", timedelta(days=2))],
    )
    with app.test_client() as client:
        data = client.get(
            "/api/health/imap", query_string={"account_emails": ACCOUNT}
        ).get_json()
        components = client.get(
            "/api/health/status", query_string={"account_emails": ACCOUNT}
        ).get_json()["components"]

    assert data["status"] == "ok"
    assert data["dead_letter_count_24h"] == 0
    assert data["dead_letter_count"] == 0
    imap = next(item for item in components if item["name"] == "IMAP")
    assert imap["status"] == "ok"


def test_a_recent_dead_letter_still_degrades_imap(tmp_path: Path) -> None:
    app = _app_with_imap_events(
        tmp_path,
        [("success", timedelta(seconds=30)), ("dead_letter", timedelta(hours=1))],
    )
    with app.test_client() as client:
        data = client.get(
            "/api/health/imap", query_string={"account_emails": ACCOUNT}
        ).get_json()

    assert data["status"] == "degraded"
    assert data["dead_letter_count_24h"] == 1
