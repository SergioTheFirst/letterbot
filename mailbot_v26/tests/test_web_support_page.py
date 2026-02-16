from __future__ import annotations

from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import SupportMethod, SupportSettings, create_app


def _build_app(tmp_path: Path, support: SupportSettings) -> object:
    db_path = tmp_path / "support.sqlite"
    KnowledgeDB(db_path)
    return create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=support,
    )


def test_support_page_requires_auth(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[SupportMethod("card", "Карта", "", "", "2202", "", "", "")],
        ),
    )
    with app.test_client() as client:
        response = client.get("/support")
        assert response.status_code in {302, 403}


def test_support_page_renders_methods(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod("card", "Карта", "", "", "2202 20XX XXXX XXXX", "", "", ""),
                SupportMethod("sbp", "СБП", "По номеру телефона", "+70000000000", "", "", "", ""),
                SupportMethod("yoomoney", "ЮMoney", "", "", "", "https://yoomoney.ru/to/abc", "", ""),
            ],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "2202 20XX XXXX XXXX" in body
        assert "+70000000000" in body
        assert "https://yoomoney.ru/to/abc" in body
        assert "Скопировать" in body


def test_support_page_hides_when_disabled(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(
            enabled=False,
            show_in_nav=False,
            methods=[],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        assert response.status_code == 404

        home = client.get("/")
        body = home.get_data(as_text=True)
        assert "Support" not in body
