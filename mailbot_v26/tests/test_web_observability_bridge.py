from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


FORBIDDEN = ["no data", "nothing to show", "all quiet", "нет данных"]


def _build_app(tmp_path: Path):
    db_path = tmp_path / "bridge.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_bridge_requires_auth_and_renders_blocks(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")

        login_resp = client.post("/login", data={"password": "pw"})
        assert login_resp.status_code in (302, 303)

        page = client.get("/")
        assert page.status_code == 200
        body = page.get_data(as_text=True)
        assert "Digest Today" in body
        assert "Recent Mail Activity" in body
        lowered = body.lower()
        for phrase in FORBIDDEN:
            assert phrase not in lowered


def test_dashboard_vars_precedence(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})

        page = client.get("/?window_days=30&limit=25")
        text = page.get_data(as_text=True)
        assert "value=\"30\" selected" in text
        assert "value=\"25\" selected" in text

        page = client.get("/")
        text = page.get_data(as_text=True)
        assert "value=\"30\" selected" in text
        assert "value=\"25\" selected" in text

        page = client.get("/?window_days=7")
        text = page.get_data(as_text=True)
        assert "value=\"7\" selected" in text


def test_share_link_button_present(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        page = client.get("/")
        body = page.get_data(as_text=True)
        assert "copy-share-link" in body
