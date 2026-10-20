"""C19: /login?next= only ever redirects to a same-site relative path."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _app(tmp_path: Path):
    db_path = tmp_path / "next.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


@pytest.mark.parametrize(
    "next_value",
    [
        "/\\evil.example",
        "/\\/evil.example",
        "\\\\evil.example",
        "//evil.example",
        "///evil.example",
        "https://evil.example/",
        "http:/evil.example",
        "javascript:alert(1)",
        "evil.example",
        "/\t/evil.example",
        "/\n/evil.example",
        "",
    ],
)
def test_login_next_rejects_off_site_targets(tmp_path: Path, next_value: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login?next=" + quote(next_value, safe=""))
    assert response.status_code == 302
    assert response.headers["Location"] == "/"


@pytest.mark.parametrize(
    "next_value",
    ["/doctor", "/archive?window=7&page=2", "/email/12"],
)
def test_login_next_keeps_relative_paths(tmp_path: Path, next_value: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login?next=" + quote(next_value, safe=""))
    assert response.status_code == 302
    assert response.headers["Location"] == next_value
