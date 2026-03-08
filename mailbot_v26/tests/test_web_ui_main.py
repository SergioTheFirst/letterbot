from __future__ import annotations

import sys
from pathlib import Path

from mailbot_v26.web_observability import app as web_app


def test_web_main_uses_settings_ini_host_port(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text(
        "[web]\nhost=127.0.0.1\nport=9911\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8"
    )

    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
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
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

    captured: dict[str, object] = {}

    class DummyApp:
        def run(
            self,
            *,
            host: str,
            port: int,
            debug: bool,
            use_reloader: bool,
            threaded: bool,
        ) -> None:
            captured["host"] = host
            captured["port"] = port

    monkeypatch.setattr(web_app, "create_app", lambda **_kwargs: DummyApp())
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    web_app.main()

    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 9911


def test_web_main_reports_busy_port_without_traceback(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[web]\nhost=127.0.0.1\nport=8787\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8"
    )

    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
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
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

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


def test_load_web_ui_settings_falls_back_when_yaml_missing_web_ui(
    tmp_path: Path,
) -> None:
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text("storage:\n  db_path: db.sqlite\n", encoding="utf-8")

    settings = web_app._load_web_ui_settings(config_yaml)

    assert settings.enabled is True
    assert settings.bind == "127.0.0.1"
    assert settings.port == 8787


def test_load_web_ui_settings_falls_back_when_yaml_invalid(tmp_path: Path) -> None:
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text("web_ui: [", encoding="utf-8")

    settings = web_app._load_web_ui_settings(config_yaml)

    assert settings.enabled is True
    assert settings.bind == "127.0.0.1"
    assert settings.port == 8787


def test_web_main_password_precedence_env_over_yaml_and_ini(
    monkeypatch, tmp_path: Path
) -> None:
    (tmp_path / "settings.ini").write_text(
        """[web]
host=127.0.0.1
port=8787
[web_ui]
password=ini-pass
""",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        """[acc]
login=u
password=p
host=h
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("WEB_PASSWORD", "env-pass")
    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            True, "127.0.0.1", 8787, "yaml-pass", "", False, [], False, False
        ),
    )
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

    captured: dict[str, object] = {}

    class DummyApp:
        def run(self, **_kwargs) -> None:
            return

    def _create_app(**kwargs):
        captured.update(kwargs)
        return DummyApp()

    monkeypatch.setattr(web_app, "create_app", _create_app)
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    web_app.main()

    assert captured["password"] == "env-pass"


def test_web_main_password_precedence_ini_then_yaml(
    monkeypatch, tmp_path: Path
) -> None:
    (tmp_path / "settings.ini").write_text(
        """[web]
host=127.0.0.1
port=8787
[web_ui]
password=ini-pass
""",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        """[acc]
login=u
password=p
host=h
""",
        encoding="utf-8",
    )

    monkeypatch.delenv("WEB_PASSWORD", raising=False)
    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            True, "127.0.0.1", 8787, "yaml-pass", "", False, [], False, False
        ),
    )
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

    captured: dict[str, object] = {}

    class DummyApp:
        def run(self, **_kwargs) -> None:
            return

    monkeypatch.setattr(
        web_app, "create_app", lambda **kwargs: captured.update(kwargs) or DummyApp()
    )
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    web_app.main()

    assert captured["password"] == "ini-pass"


def test_web_main_password_falls_back_to_ini_and_warns_when_empty(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    (tmp_path / "settings.ini").write_text(
        """[web]
host=127.0.0.1
port=8787
[web_ui]
password=
""",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        """[acc]
login=u
password=p
host=h
""",
        encoding="utf-8",
    )

    monkeypatch.delenv("WEB_PASSWORD", raising=False)
    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            True, "127.0.0.1", 8787, "", "", False, [], False, False
        ),
    )
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

    captured: dict[str, object] = {}

    class DummyApp:
        def run(self, **_kwargs) -> None:
            return

    monkeypatch.setattr(
        web_app, "create_app", lambda **kwargs: captured.update(kwargs) or DummyApp()
    )
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    with caplog.at_level("WARNING"):
        web_app.main()

    assert captured["password"] == ""
    assert "web_ui_password_not_configured_using_empty_password" in caplog.text


def test_web_main_passes_local_smoke_bypass_from_ini(
    monkeypatch, tmp_path: Path, caplog
) -> None:
    (tmp_path / "settings.ini").write_text(
        """[web]
host=127.0.0.1
port=8787
[web_ui]
password=ini-pass
allow_local_smoke_bypass=true
""",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        """[acc]
login=u
password=p
host=h
""",
        encoding="utf-8",
    )

    monkeypatch.delenv("WEB_PASSWORD", raising=False)
    monkeypatch.setattr(web_app, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: tmp_path / "config.yaml",
    )
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            True, "127.0.0.1", 8787, "yaml-pass", "", False, [], False, False
        ),
    )
    monkeypatch.setattr(
        web_app,
        "_load_support_settings",
        lambda _path: web_app.SupportSettings(False, False, []),
    )
    monkeypatch.setattr(
        web_app, "_load_web_ui_secrets", lambda _config_dir: ("secret", 1.0)
    )
    monkeypatch.setattr(
        web_app,
        "load_storage_config",
        lambda _config_dir: type("S", (), {"db_path": tmp_path / "db.sqlite"})(),
    )

    captured: dict[str, object] = {}

    class DummyApp:
        def run(self, **_kwargs) -> None:
            return

    monkeypatch.setattr(
        web_app, "create_app", lambda **kwargs: captured.update(kwargs) or DummyApp()
    )
    monkeypatch.setattr(sys, "argv", ["web_app", "--config", str(tmp_path)])

    with caplog.at_level("WARNING"):
        web_app.main()

    assert captured["allow_local_smoke_bypass"] is True
    assert "WEB_UI_LOCAL_SMOKE_BYPASS_ACTIVE" in caplog.text
