from __future__ import annotations

from pathlib import Path

from mailbot_v26.pipeline import processor as processor_module
from mailbot_v26.pipeline.processor import EmailContext
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.version import __version__, get_version
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.web_observability.flask_stub import render_template


def test_import_and_version_source_of_truth() -> None:
    assert __version__
    assert get_version() == __version__


def test_web_cockpit_and_template_show_version(tmp_path: Path) -> None:
    rendered = render_template(
        "mailbot_v26/web_observability/templates/base.html", app_version=get_version()
    )
    assert f"LetterBot.ru v{__version__}" in rendered

    db_path = tmp_path / "observability.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="password123", secret_key="secret")

    with app.test_client() as client:
        with client.session_transaction() as session:
            session["authenticated"] = True
            session["csrf_token"] = "token"
        response = client.get("/cockpit", follow_redirects=True)

    assert response.status_code == 200
    assert f"LetterBot.ru v{__version__}" in response.get_data(as_text=True)


def test_tg_payload_generation_smoke() -> None:
    payload = processor_module._build_telegram_text(
        priority="🟡",
        from_email="hq@example.com",
        subject="Smoke",
        action_line="Проверить письмо",
        body_summary="Тестовый текст",
        body_text="",
        attachment_summary="",
    )
    validated = processor_module.validate_tg_payload(
        payload,
        EmailContext(
            subject="Smoke",
            from_email="hq@example.com",
            body_text="Тестовый текст",
            attachments_count=0,
        ),
    )
    assert "Smoke" in validated


def test_examples_are_readable() -> None:
    settings_text = Path("mailbot_v26/config/settings.ini.example").read_text(
        encoding="utf-8"
    )
    accounts_text = Path("mailbot_v26/config/accounts.ini.example").read_text(
        encoding="utf-8"
    )

    assert "[general]" in settings_text
    assert "[example_account]" in accounts_text
