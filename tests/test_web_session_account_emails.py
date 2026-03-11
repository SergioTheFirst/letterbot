from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import create_app, resolve_dashboard_vars


def _request_with_args(**query: object) -> SimpleNamespace:
    return SimpleNamespace(args=query)


def _insert_email_samples(db_path: Path) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (
                id, account_email, from_email, received_at, priority, action_line, body_summary
            )
            VALUES
                (1, 'user@mail.ru', 'critical@acme.com', ?, '🔴', 'Critical task', 'urgent')
            """,
            (now_iso,),
        )


def test_parse_accounts_from_list_session() -> None:
    session = {"dashboard_vars": {"account_emails": ["user@mail.ru"]}}

    resolved = resolve_dashboard_vars(_request_with_args(), session)

    assert resolved.account_emails == ["user@mail.ru"]


def test_parse_accounts_idempotent_after_two_requests() -> None:
    session: dict[str, object] = {}

    first = resolve_dashboard_vars(
        _request_with_args(account_emails="user@mail.ru"),
        session,
    )
    second = resolve_dashboard_vars(_request_with_args(), session)

    assert first.account_emails == ["user@mail.ru"]
    assert second.account_emails == ["user@mail.ru"]
    assert all("['" not in item for item in second.account_emails)


def test_clean_email_list_strips_repr_garbage() -> None:
    session: dict[str, object] = {}

    resolve_dashboard_vars(
        _request_with_args(account_emails="['user@mail.ru'],good@mail.ru,['bad']"),
        session,
    )

    assert session["dashboard_vars"]["account_emails"] == ["good@mail.ru"]


def test_scope_hint_not_shown_for_repr_garbage(tmp_path: Path) -> None:
    db_path = tmp_path / "scope.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/?account_email=%5B'user%40mail.ru'%5D&window_days=7")

    body = page.get_data(as_text=True)
    assert "['user@mail.ru'] • last 7 days" not in body


def test_lane_activity_rows_returns_data_with_correct_email(tmp_path: Path) -> None:
    db_path = tmp_path / "lane.sqlite"
    KnowledgeDB(db_path)
    _insert_email_samples(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        page = client.get("/?account_emails=user@mail.ru&window_days=7&lane=critical")

    body = page.get_data(as_text=True)
    assert "Live mail stream" in body
    assert "critical@acme.com" not in body
