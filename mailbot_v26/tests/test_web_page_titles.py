"""Every web page names itself in the browser tab (P3b D2).

Most pages used the generic APP_TITLE ("Observability Console") as their
<title>, so every tab of the cockpit looked the same.
"""

from __future__ import annotations

import html
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import SupportSettings, create_app

ACCOUNT = "acct@example.com"
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)

EXPECTED_TITLES = {
    "/": "Home · LetterBot.ru",
    "/dashboard": "Home · LetterBot.ru",
    "/archive": "Archive · LetterBot.ru",
    "/commitments": "Commitments · LetterBot.ru",
    "/health": "Health · LetterBot.ru",
    "/events": "Events · LetterBot.ru",
    "/doctor": "Doctor · LetterBot.ru",
    "/latency": "Latency · LetterBot.ru",
    "/attention": "Attention · LetterBot.ru",
    "/learning": "Learning · LetterBot.ru",
    "/relationships": "Relationships · LetterBot.ru",
    "/relationships/contact?contact_id=c1": "Contact · LetterBot.ru",
    "/email/1": "Email 1 · LetterBot.ru",
    "/support": "Support · LetterBot.ru",
}


def _app(tmp_path: Path):
    db_path = tmp_path / "titles.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO emails (id, account_email, from_email, subject, received_at) "
            "VALUES (1, ?, 'a@example.com', 's', ?)",
            (ACCOUNT, now),
        )
        conn.commit()
    return create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=True, show_in_nav=True, methods=[]),
    )


def _title(body: str) -> str:
    match = _TITLE_RE.search(body)
    assert match is not None, "page has no <title>"
    return html.unescape(" ".join(match.group(1).split()))


@pytest.mark.parametrize(("path", "expected"), sorted(EXPECTED_TITLES.items()))
def test_page_title_names_the_page(tmp_path: Path, path: str, expected: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(path)
    assert response.status_code == 200
    assert _title(response.get_data(as_text=True)) == expected


def test_every_html_route_has_its_own_title(tmp_path: Path) -> None:
    app = _app(tmp_path)
    titles: dict[str, str] = {}
    with app.test_client() as client:
        for rule in app.url_map.iter_rules():
            if "GET" not in (rule.methods or set()) or rule.arguments:
                continue
            if rule.endpoint in {"static", "login"} or rule.rule.startswith("/partial/"):
                continue
            response = client.get(rule.rule)
            if "text/html" not in (response.content_type or ""):
                continue
            if response.status_code != 200:
                continue
            titles[rule.rule] = _title(response.get_data(as_text=True))

    assert titles, "no HTML routes rendered"
    generic = sorted(path for path, title in titles.items() if "Observability Console" in title)
    assert generic == []
    unbranded = sorted(path for path, title in titles.items() if not title.endswith(" · LetterBot.ru"))
    assert unbranded == []
