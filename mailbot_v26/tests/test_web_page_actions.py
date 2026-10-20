"""Page actions render and the Basic/Engineer toggle is reversible (P3b D1/C5).

base.html wrapped {% block page_actions %} in a condition on a context
variable no view ever set, so the mode toggle and "Export CSV" never
rendered; once ?mode=engineer was visited the session kept engineer mode
with no visible way back.
"""

from __future__ import annotations

import html
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"


def _app(tmp_path: Path):
    db_path = tmp_path / "page-actions.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO emails (id, account_email, from_email, subject, received_at) "
            "VALUES (1, ?, 'a@example.com', 's', ?)",
            (ACCOUNT, now),
        )
        conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _page_actions(body: str) -> str:
    match = re.search(r'<div class="page-actions">(.*?)</div>\s*</div>', body, re.S)
    return match.group(1) if match else ""


def _mode_link(body: str, label: str) -> str:
    match = re.search(
        r'<a class="mode-pill[^"]*" href="([^"]+)">' + re.escape(label) + "</a>", body
    )
    assert match is not None, f"{label} toggle missing"
    return html.unescape(match.group(1))


def test_attention_page_shows_the_csv_export(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        body = client.get("/attention", query_string={"account_emails": ACCOUNT}).get_data(
            as_text=True
        )

    actions = _page_actions(body)
    assert "Export CSV" in actions
    assert "/attention.csv?" in actions


def test_mode_toggle_renders_where_the_mode_changes_the_page(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        pages = {
            path: client.get(path).get_data(as_text=True)
            for path in ("/", "/health", "/email/1")
        }
        archive = client.get("/archive").get_data(as_text=True)

    for path, body in pages.items():
        actions = _page_actions(body)
        assert ">Basic</a>" in actions, path
        assert ">Engineer</a>" in actions, path
    # The archive ignores the mode, so it offers no toggle.
    assert ">Engineer</a>" not in archive


def test_engineer_mode_can_be_switched_back_to_basic(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        engineer = client.get("/?mode=engineer").get_data(as_text=True)
        assert 'data-testid="engineer-blocks"' in engineer

        # The mode follows the user to other pages...
        health = client.get("/health").get_data(as_text=True)
        assert 'data-testid="health-engineer-block"' in health
        assert re.search(r'mode-pill active" href="[^"]+">Engineer</a>', health)

        # ...and the visible Basic toggle turns it off everywhere.
        basic = client.get(_mode_link(health, "Basic")).get_data(as_text=True)
        assert 'data-testid="health-engineer-block"' not in basic
        home = client.get("/").get_data(as_text=True)
        assert 'data-testid="engineer-blocks"' not in home
        assert re.search(r'mode-pill active" href="[^"]+">Basic</a>', home)
