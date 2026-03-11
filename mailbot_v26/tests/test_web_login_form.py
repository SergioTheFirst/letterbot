from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _build_app(tmp_path: Path):
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_login_route_redirects_to_home(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login")
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/")


def test_login_next_l_redirects_to_home_alias(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login?next=/l")
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/")


def test_legacy_l_alias_redirects_to_index(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/l")
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/")


def test_default_loopback_accessible_without_login(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/")
        assert response.status_code == 200


def test_default_cidr_blocks_non_local(tmp_path: Path) -> None:
    app = _build_app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login", environ_base={"REMOTE_ADDR": "203.0.113.10"})
        assert response.status_code == 403


def test_local_smoke_bypass_allows_loopback_without_login(tmp_path: Path) -> None:
    db_path = tmp_path / "web-bypass.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_local_smoke_bypass=True,
    )
    with app.test_client() as client:
        response = client.get("/")
        assert response.status_code == 200


def test_local_smoke_bypass_not_global_for_public_ip(tmp_path: Path) -> None:
    db_path = tmp_path / "web-bypass-public.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_local_smoke_bypass=True,
    )
    with app.test_client() as client:
        response = client.get("/", environ_base={"REMOTE_ADDR": "198.51.100.42"})
        assert response.status_code == 403


def test_local_smoke_bypass_accepts_trusted_forwarded_loopback(tmp_path: Path) -> None:
    db_path = tmp_path / "web-bypass-forwarded.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        allow_local_smoke_bypass=True,
        web_ui_bind="127.0.0.1",
    )
    with app.test_client() as client:
        response = client.get(
            "/",
            environ_base={"REMOTE_ADDR": "172.17.0.10"},
            headers={"X-Forwarded-For": "127.0.0.1"},
        )
        assert response.status_code == 200
