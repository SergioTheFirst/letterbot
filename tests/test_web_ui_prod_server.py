from __future__ import annotations

from pathlib import Path

import pytest

from mailbot_v26 import deps
from mailbot_v26.web_observability import app as web_app


def test_web_ui_prod_dependency_guard_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "has", lambda module: module != "waitress")

    with pytest.raises(deps.DependencyError) as exc_info:
        deps.require_runtime_for("web_ui_prod")

    message = str(exc_info.value)
    assert "Missing dependency: waitress" in message
    assert "python -m pip install waitress" in message


def test_web_ui_main_fails_cleanly_when_waitress_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        web_app,
        "require_runtime_for",
        lambda mode="runtime": (
            (_ for _ in ()).throw(
                deps.DependencyError(
                    "Missing dependency: waitress. Install: python -m pip install waitress"
                )
            )
            if mode == "web_ui_prod"
            else None
        ),
    )
    monkeypatch.setattr(
        web_app,
        "_resolve_yaml_config_path",
        lambda _path, _config_dir: Path("config.yaml"),
    )
    monkeypatch.setattr(
        web_app,
        "_load_web_ui_settings",
        lambda _path: web_app.WebUISettings(
            enabled=True,
            bind="127.0.0.1",
            port=8787,
            password="strong_password",
            api_token="",
            allow_lan=False,
            allow_cidrs=[],
            prod_server=True,
            require_strong_password_on_lan=True,
        ),
    )
    monkeypatch.setattr(
        web_app.argparse.ArgumentParser,
        "parse_args",
        lambda _self: web_app.argparse.Namespace(
            db=None, config=Path("."), config_yaml=None, bind=None, port=None
        ),
    )

    with pytest.raises(deps.DependencyError) as exc_info:
        web_app.main()

    assert "waitress" in str(exc_info.value)
