import re
import sqlite3
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


BANNED_PHRASES = [
    "No " + "data",
    "Nothing " + "to show",
    "All " + "quiet",
]


def _build_app(tmp_path: Path) -> tuple[Path, object]:
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    return db_path, create_app(db_path=db_path, password="pw", secret_key="secret")


def _assert_no_banned_phrases(body: str) -> None:
    for phrase in BANNED_PHRASES:
        assert re.search(re.escape(phrase), body, flags=re.IGNORECASE) is None


def test_banned_phrases_not_present(tmp_path: Path) -> None:
    db_path, app = _build_app(tmp_path)
    with app.test_client() as client:
        login_page = client.get("/login")
        _assert_no_banned_phrases(login_page.get_data(as_text=True))

        login_with_csrf(client, "pw")
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO emails (id, account_email, from_email, received_at)
                VALUES (1, 'acct@example.com', 'sender@example.com', '2026-01-01T00:00:00+00:00')
                """
            )
        for path in [
            "/",
            "/latency",
            "/health",
            "/archive",
            "/email/1",
            "/events",
            "/commitments",
        ]:
            response = client.get(path)
            assert response.status_code == 200
            _assert_no_banned_phrases(response.get_data(as_text=True))
