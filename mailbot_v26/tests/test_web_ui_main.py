from __future__ import annotations

import sys
from pathlib import Path

from mailbot_v26.web_observability import app as web_app


def test_web_main_uses_settings_ini_host_port(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=127.0.0.1\nport=9911\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text("[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8")

    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(web_app, "_resolve_yaml_config_path", lambda _path, _config_dir: tmp_path / "config.yaml")
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            enabled=True,
            bind="127.0.0.1",
            port=8787,
            password="pw",
            api_token="",
            allow_lan=False,
            allow_cidrs=[],
            prod_server=False,
            require_strong_password_on_lan=False,
        ),
    )
    monkeypatch.setattr(web_app, "_load_support_settings", lambda _path: web_app.SupportSettings(False, False, []))
    monkeypatch.setattr(web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0))
    monkeypatch.setattr(web_app, "load_storage_config", lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})())

    captured: dict[str, object] = {}

    class DummyApp:
        def run(self, *, host: str, port: int, debug: bool, use_reloader: bool, threaded: bool) -> None:
            captured["host"] = host
            captured["port"] = port

    monkeypatch.setattr(web_app, "create_app", lambda **_kwargs: DummyApp())
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    web_app.main()

    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 9911


def test_web_main_reports_busy_port_without_traceback(monkeypatch, tmp_path: Path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=127.0.0.1\nport=8787\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text("[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8")

    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(web_app, "_resolve_yaml_config_path", lambda _path, _config_dir: tmp_path / "config.yaml")
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            enabled=True,
            bind="127.0.0.1",
            port=8787,
            password="pw",
            api_token="",
            allow_lan=False,
            allow_cidrs=[],
            prod_server=False,
            require_strong_password_on_lan=False,
        ),
    )
    monkeypatch.setattr(web_app, "_load_support_settings", lambda _path: web_app.SupportSettings(False, False, []))
    monkeypatch.setattr(web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0))
    monkeypatch.setattr(web_app, "load_storage_config", lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})())

    class BusyApp:
        def run(self, **_kwargs) -> None:
            raise OSError("Address already in use")

    monkeypatch.setattr(web_app, "create_app", lambda **_kwargs: BusyApp())
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    try:
        web_app.main()
        raised = None
    except SystemExit as exc:
        raised = exc

    out = capsys.readouterr().out
    assert isinstance(raised, SystemExit)
    assert raised.code == 1
    assert "Порт 8787 занят" in out
