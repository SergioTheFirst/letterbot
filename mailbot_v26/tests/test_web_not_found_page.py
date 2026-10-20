"""An unknown address shows a 404 page inside the app shell (P3b D2).

Flask's bare "Not Found" page had no navigation back into the cockpit. HTML
paths now get the normal page frame with links home; /api/ paths get JSON.
"""

from __future__ import annotations

from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

MARKER = "zz-unknown-page-7q"


def _app(tmp_path: Path):
    db_path = tmp_path / "not-found.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_unknown_page_renders_inside_the_app_shell(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(f"/{MARKER}")

    body = response.get_data(as_text=True)
    assert response.status_code == 404
    assert "text/html" in response.content_type
    assert 'class="app-shell"' in body
    assert 'class="nav"' in body
    assert 'data-testid="not-found"' in body
    assert "<title>Page not found · LetterBot.ru</title>" in body
    assert '<h1>Page not found</h1>' in body
    assert 'href="/"' in body
    # The requested address is never reflected back into the page.
    assert MARKER not in body
    # A missing page has no data scope to filter.
    assert 'class="dashboard-vars"' not in body
    assert response.headers.get("X-Content-Type-Options") == "nosniff"


def test_unknown_api_path_answers_json(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(f"/api/{MARKER}")

    assert response.status_code == 404
    assert response.get_json() == {"error": "not found"}
    assert MARKER not in response.get_data(as_text=True)


def test_remote_client_without_login_never_sees_the_404_page(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(
            f"/{MARKER}", environ_base={"REMOTE_ADDR": "192.168.1.5"}
        )

    # The access guard runs before routing: no page content leaks to the LAN.
    assert response.status_code in {302, 303, 403}
    assert 'data-testid="not-found"' not in response.get_data(as_text=True)
