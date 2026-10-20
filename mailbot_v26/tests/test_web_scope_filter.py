"""The global scope filter bar (P3b D2).

The "PII" checkbox of the filter bar was a no-op: the web never shows raw
senders or subjects whatever it says (CONSTITUTION IV), so it only suggested
a privacy switch that does not exist.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"


def _app(tmp_path: Path, *, allow_pii: bool = False):
    db_path = tmp_path / "scope-filter.sqlite"
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
        db_path=db_path, password="pw", secret_key="secret", allow_pii=allow_pii
    )


def _scope_form(body: str) -> str:
    match = re.search(r'<form[^>]*class="dashboard-vars"[^>]*>(.*?)</form>', body, re.S)
    assert match is not None, "global filter bar missing"
    return match.group(1)


@pytest.mark.parametrize("path", ["/", "/health", "/events"])
def test_filter_bar_has_no_pii_checkbox_even_when_pii_is_allowed(
    tmp_path: Path, path: str
) -> None:
    app = _app(tmp_path, allow_pii=True)
    with app.test_client() as client:
        body = client.get(path, query_string={"pii": "1"}).get_data(as_text=True)

    form = _scope_form(body)
    assert 'type="checkbox"' not in form
    assert 'name="pii"' not in form
    assert ">PII<" not in body
    assert 'name="window_days"' in form
