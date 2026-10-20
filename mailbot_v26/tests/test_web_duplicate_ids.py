"""No HTML page of the web cockpit repeats an element id (P3b D1/C6).

Duplicate ids break label/for pairs, scripts (getElementById returns the
first match only) and assistive technology. The crawl renders every GET
route, with and without an account scope, in engineer mode and on the
per-email and per-contact pages, against a seeded database.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "acct@example.com"
EMAIL_ID = 7
ENTITY_ID = "0b6f3c2e-8f1d-4f0a-9d65-2c7e1b9a4d33"
SCOPE = f"account_emails={ACCOUNT}&account_email={ACCOUNT}"


class _IdCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: Counter[str] = Counter()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name == "id" and value:
                self.ids[value] += 1

    handle_startendtag = handle_starttag


def _seed(tmp_path: Path) -> Path:
    db_path = tmp_path / "ids.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, priority,
                                llm_provider, action_line, body_summary,
                                received_at, created_at)
            VALUES (?, ?, 'sender@example.com', 'Subject', '🔴', 'gigachat',
                    'Pay invoice', 'Summary', ?, ?)
            """,
            (EMAIL_ID, ACCOUNT, now.isoformat(), now.isoformat()),
        )
        events = (
            ("email_received", {"from_email": "sender@example.com"}),
            (
                "message_interpretation",
                {
                    "doc_kind": "invoice",
                    "amount": 100,
                    "priority": "red",
                    "action": "Pay",
                    "confidence": 0.9,
                    "issuer_label": "Vendor",
                },
            ),
            ("telegram_delivered", {"delivery_mode": "IMMEDIATE"}),
            ("trust_score_updated", {"trust_score": 0.8}),
            ("imap_health_v1", {"subtype": "success", "detail": "ok"}),
        )
        for index, (event_type, payload) in enumerate(events):
            payload_json = json.dumps(payload)
            conn.execute(
                """
                INSERT INTO events_v1 (event_type, ts_utc, ts, account_id,
                                       entity_id, email_id, payload, payload_json,
                                       schema_version, fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    event_type,
                    now.timestamp() + index,
                    now.isoformat(),
                    ACCOUNT,
                    ENTITY_ID,
                    EMAIL_ID,
                    payload_json,
                    payload_json,
                    f"ids:{event_type}",
                ),
            )
        conn.execute(
            """
            INSERT INTO processing_spans (span_id, ts_start_utc, ts_end_utc,
                                          total_duration_ms, account_id, email_id,
                                          llm_provider, llm_model, outcome,
                                          error_code)
            VALUES ('span-ids', ?, ?, 1500, ?, ?, 'gigachat', 'giga-pro', 'error',
                    'LLM_TIMEOUT')
            """,
            (now.timestamp() - 2, now.timestamp(), ACCOUNT, EMAIL_ID),
        )
        conn.commit()
    return db_path


def _html_paths(app) -> list[str]:
    paths: set[str] = set()
    for rule in app.url_map.iter_rules():
        if "GET" not in (rule.methods or set()) or rule.arguments:
            continue
        if rule.endpoint in {"static", "login"}:
            continue
        paths.update({rule.rule, f"{rule.rule}?{SCOPE}"})
    paths.update(
        {
            f"/?{SCOPE}&mode=engineer",
            f"/health?{SCOPE}&mode=engineer",
            f"/email/{EMAIL_ID}?{SCOPE}",
            f"/email/{EMAIL_ID}?{SCOPE}&mode=engineer",
            f"/archive?{SCOPE}&message_id={EMAIL_ID}",
            f"/relationships/contact?{SCOPE}&contact_id=contact:{ENTITY_ID}",
        }
    )
    return sorted(paths)


def test_no_page_repeats_an_element_id(tmp_path: Path) -> None:
    app = create_app(db_path=_seed(tmp_path), password="pw", secret_key="secret")
    duplicates: dict[str, list[str]] = {}
    pages_checked = 0
    with app.test_client() as client:
        for path in _html_paths(app):
            response = client.get(path)
            if "text/html" not in (response.content_type or ""):
                continue
            pages_checked += 1
            collector = _IdCollector()
            collector.feed(response.get_data(as_text=True))
            repeated = sorted(key for key, count in collector.ids.items() if count > 1)
            if repeated:
                duplicates[path] = repeated

    assert pages_checked >= 20
    assert duplicates == {}
