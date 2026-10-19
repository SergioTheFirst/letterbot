"""QW21 (finding C18): no shared constant Flask secret when none is configured."""

from __future__ import annotations

import re
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import _load_web_ui_secrets, create_app

OLD_CONSTANT = "letterbot-local-web-secret"


def test_missing_secret_gives_random_key_per_app(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("WEB_SECRET_KEY", raising=False)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    db_path = tmp_path / "secret.sqlite"
    KnowledgeDB(db_path)

    first_key, _ = _load_web_ui_secrets(config_dir)
    second_key, _ = _load_web_ui_secrets(config_dir)
    first_app = create_app(db_path=db_path, password="pw", secret_key=first_key)
    second_app = create_app(db_path=db_path, password="pw", secret_key=second_key)

    assert first_app.secret_key != second_app.secret_key
    for key in (first_app.secret_key, second_app.secret_key):
        assert key != OLD_CONSTANT
        assert re.fullmatch(r"[0-9a-f]{64}", str(key))
    assert not any(config_dir.iterdir()), "fallback key must not be written to disk"


def test_configured_secret_is_kept(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WEB_SECRET_KEY", "configured-secret")
    key, _ = _load_web_ui_secrets(tmp_path)
    assert key == "configured-secret"
