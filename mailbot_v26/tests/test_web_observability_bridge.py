from pathlib import Path


from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from datetime import datetime, timezone
from urllib.parse import quote
import sqlite3
from mailbot_v26.tests._web_helpers import login_with_csrf

FORBIDDEN = [
    "nothing " + "to show",
    "all " + "quiet",
]


def _build_app(tmp_path: Path):
    db_path = tmp_path / "bridge.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_cockpit_renders_blocks_without_login(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/")
        assert response.status_code == 200
        page = response
        assert page.status_code == 200
        body = page.get_data(as_text=True)
        assert "Read-only observability" in body
        assert ">Обработанные письма<" in body
        lowered = body.lower()
        for phrase in FORBIDDEN:
            assert phrase not in lowered


def test_dashboard_vars_precedence(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")

        page = client.get("/?window_days=30")
        text = page.get_data(as_text=True)
        assert 'value="30" selected' in text

        page = client.get("/")
        text = page.get_data(as_text=True)
        assert 'value="30" selected' in text

        page = client.get("/?window_days=7")
        text = page.get_data(as_text=True)
        assert 'value="7" selected' in text


def test_share_link_button_present(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/")
        body = page.get_data(as_text=True)
        assert "copy-share-link" in body


def _insert_email_samples(db_path: Path) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                id, account_email, from_email, received_at, priority, action_line, body_summary
            )
            VALUES
                (1, 'acct@example.com', 'critical@acme.com', ?, '🔴', 'Critical task', 'urgent'),
                (2, 'acct@example.com', 'normal@acme.com', ?, '🔵', 'Routine update', 'status')
            """,
            (now_iso, now_iso),
        )


def test_live_dashboard_template_renders_with_scope_vars(tmp_path: Path) -> None:
    db_path = tmp_path / "bridge.sqlite"
    KnowledgeDB(db_path)
    _insert_email_samples(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/?account_emails=acct@example.com&window_days=7&limit=10")
        body = page.get_data(as_text=True)
        assert 'data-testid="live-dashboard"' in body
        assert "fetch('/api/dashboard'" in body
        assert ">Обработанные письма<" in body


def test_dashboard_renders_for_lane_query_without_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "bridge.sqlite"
    KnowledgeDB(db_path)
    _insert_email_samples(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get(
            "/?account_emails=acct@example.com&window_days=7&lane=critical"
        )
        body = page.get_data(as_text=True)
        assert page.status_code == 200
        assert 'data-testid="live-dashboard"' in body
        lowered = body.lower()
        for phrase in FORBIDDEN:
            assert phrase not in lowered


def test_share_link_includes_lane_and_vars(tmp_path: Path) -> None:
    db_path = tmp_path / "bridge.sqlite"
    KnowledgeDB(db_path)
    _insert_email_samples(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get(
            "/?account_emails=acct@example.com&window_days=30&limit=25&lane=critical"
        )
        body = page.get_data(as_text=True)
        assert "data-share-url" in body
        assert "lane=critical" in body
        assert (
            f"account_emails={quote('acct@example.com')}" in body
            or "account_emails=acct@example.com" in body
        )
        assert "window_days=30" in body
        assert "limit=25" in body
