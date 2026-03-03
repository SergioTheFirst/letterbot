from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _build_app(tmp_path: Path):
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_login_form_has_password_label(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login")
        body = response.get_data(as_text=True)
        assert '<label for="password">Password</label>' in body
        assert 'id="password"' in body
        assert 'data-testid="login-password"' in body
        assert 'name="csrf_token"' in body
        assert ">Sign in<" in body


def test_login_next_l_redirects_to_home_alias(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        login_page = client.get("/login?next=/l")
        body = login_page.get_data(as_text=True)
        import re
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        assert match
        response = client.post("/login?next=/l", data={"password": "pw", "csrf_token": match.group(1)})
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/")


def test_legacy_l_alias_redirects_to_index(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        login_page = client.get("/login")
        body = login_page.get_data(as_text=True)
        import re
        match = re.search(r'name="csrf_token" value="([^"]+)"', body)
        assert match
        client.post("/login", data={"password": "pw", "csrf_token": match.group(1)})
        response = client.get("/l")
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/")

