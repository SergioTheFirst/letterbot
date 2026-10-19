"""Web INI readers accept Notepad's "UTF-8 with BOM" files (QW20 / F9-app)."""

from __future__ import annotations

from pathlib import Path

import pytest

from mailbot_v26.web_observability.app import (
    _load_local_smoke_bypass_from_ini,
    _load_support_settings,
    _load_web_ui_secrets,
    create_app,
)


def _write_bom_ini(path: Path, text: str) -> None:
    path.write_bytes(b"\xef\xbb\xbf" + text.encode("utf-8"))


def test_web_ui_secrets_read_bom_config_ini(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WEB_SECRET_KEY", raising=False)
    _write_bom_ini(
        tmp_path / "config.ini",
        "[general]\nweb_secret_key = bom-secret\nattention_cost_per_hour = 12\n",
    )

    secret_key, attention_cost = _load_web_ui_secrets(tmp_path)

    assert secret_key == "bom-secret"
    assert attention_cost == 12.0


def test_support_settings_read_bom_settings_ini(tmp_path: Path) -> None:
    _write_bom_ini(
        tmp_path / "settings.ini",
        "[support]\nenabled = true\nshow_in_nav = true\nlabel = Support\n",
    )

    settings = _load_support_settings(tmp_path / "config.yaml")

    assert settings.enabled is True
    assert len(settings.methods) == 1


def test_smoke_bypass_reads_bom_settings_ini(tmp_path: Path) -> None:
    _write_bom_ini(
        tmp_path / "settings.ini", "[web_ui]\nallow_local_smoke_bypass = true\n"
    )

    assert _load_local_smoke_bypass_from_ini(tmp_path) is True


def test_create_app_reads_locale_from_bom_settings_ini(tmp_path: Path) -> None:
    _write_bom_ini(tmp_path / "settings.ini", "[ui]\nlocale = ru\n")
    db_path = tmp_path / "web.sqlite"
    db_path.write_bytes(b"")

    app = create_app(
        db_path=db_path, password="pw", secret_key="secret", config_dir=tmp_path
    )

    assert app.config["UI_LOCALE"] == "ru"
