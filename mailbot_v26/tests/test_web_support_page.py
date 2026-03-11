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


def test_support_page_accessible_without_login(tmp_path: Path) -> None:
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
        assert response.status_code == 200


def test_support_page_renders_methods(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod(
                    "card",
                    "Карта",
                    "Для переводов",
                    "",
                    "2202 20XX XXXX XXXX",
                    "",
                    "",
                    "",
                ),
                SupportMethod(
                    "sbp", "СБП", "По номеру телефона", "+70000000000", "", "", "", ""
                ),
                SupportMethod(
                    "yoomoney",
                    "ЮMoney",
                    "",
                    "",
                    "",
                    "https://yoomoney.ru/to/abc",
                    "",
                    "",
                ),
            ],
            text="Поддержать можно любым удобным способом.",
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Support Letterbot" in body
        assert "Letterbot остаётся бесплатным" in body
        assert "support-method-card" in body
        assert "2202 20XX XXXX XXXX" in body
        assert "+70000000000" in body
        assert 'href="https://yoomoney.ru/to/abc"' in body
        assert "support-url-cta" in body
        assert "Скопировать" in body


def test_support_page_renders_qr_when_available(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod(
                    "support",
                    "Поддержать Letterbot",
                    "",
                    "",
                    "",
                    "https://example.com/support",
                    "support.png",
                    "data:image/png;base64,abc",
                )
            ],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "support-qr-frame" in body
        assert '<img src="data:image/png;base64,abc"' in body


def test_support_page_renders_empty_state_when_no_methods(tmp_path: Path) -> None:
    app = _build_app(
        tmp_path,
        SupportSettings(enabled=True, show_in_nav=True, methods=[]),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Способы поддержки ещё не настроены" in body


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
        assert 'href="/support"' not in body
        assert "topbar-donate-qr" in body


def test_support_page_renders_from_ini_fallback(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    settings_path = tmp_path / "settings.ini"
    qr_path = tmp_path / "support.png"
    qr_path.write_bytes(
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc````\x00\x00\x00\x04\x00\x01\x0b\xe7\x02\x9d\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    settings_path.write_text(
        """
[support]
enabled = true
show_in_nav = true
label = Поддержать Letterbot
text = Поддержать проект можно по ссылке или QR-коду
url = https://example.com/support
details = Boosty / СБП
qr_image = support.png
""",
        encoding="utf-8",
    )

    from mailbot_v26.web_observability.app import _load_support_settings

    app = _build_app(tmp_path, _load_support_settings(config_path))
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/support")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Поддержать Letterbot" in body
        assert "Поддержать проект можно по ссылке или QR-коду" in body
        assert "https://example.com/support" in body
        assert '<img src="data:image/png;base64,' in body
