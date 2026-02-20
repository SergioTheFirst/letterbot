from __future__ import annotations

from pathlib import Path

import pytest

from mailbot_v26 import config_yaml, start
from mailbot_v26.tools import config_bootstrap


def _valid_config_dict(schema_version: int | None = 1) -> dict[str, object]:
    payload: dict[str, object] = {
        "telegram": {"bot_token": "token", "chat_id": "123"},
        "llm": {
            "provider": "cloudflare",
            "cloudflare": {"api_token": "api", "account_id": "account"},
        },
        "accounts": [
            {
                "name": "Work",
                "email": "work@example.com",
                "imap_host": "imap.example.com",
                "imap_port": 993,
                "username": "work@example.com",
                "password": "secret",
                "enabled": True,
            }
        ],
    }
    if schema_version is not None:
        payload["schema_version"] = schema_version
    return payload


def test_missing_schema_version_defaults_to_one() -> None:
    cfg = _valid_config_dict(schema_version=None)

    assert config_yaml.get_schema_version(cfg) == 1
    ok, error = config_yaml.validate_config(cfg)
    assert ok
    assert error == ""


def test_validate_config_compat_fails_for_newer_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(config_bootstrap, "load_yaml_config", lambda _path: _valid_config_dict(schema_version=999))

    code = config_bootstrap.run_validate_config(base_dir=tmp_path, compat=True)
    output = capsys.readouterr().out

    assert code == 2
    assert "Status: FAIL" in output
    assert config_yaml.SCHEMA_NEWER_MESSAGE in output


def test_validate_config_compat_prints_single_hint_for_older_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(config_yaml, "SUPPORTED_SCHEMA_VERSION", 2)
    monkeypatch.setattr(config_bootstrap, "SUPPORTED_SCHEMA_VERSION", 2)
    monkeypatch.setattr(config_bootstrap, "load_yaml_config", lambda _path: _valid_config_dict(schema_version=1))

    code = config_bootstrap.run_validate_config(base_dir=tmp_path, compat=True)
    output = capsys.readouterr().out

    hint_lines = [line for line in output.splitlines() if line.startswith("Hint:")]
    assert code == 0
    assert "Status: OK" in output
    assert len(hint_lines) == 1
    assert hint_lines[0] == f"Hint: {config_yaml.SCHEMA_OLDER_HINT}"


def test_start_schema_mismatch_exits_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "config.yaml"
    monkeypatch.setattr(start, "load_yaml_config", lambda _path: _valid_config_dict(schema_version=999))

    with pytest.raises(SystemExit) as exc_info:
        start._load_yaml_config_or_exit(config_path)

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert exc_info.value.code == 2
    assert config_yaml.SCHEMA_NEWER_MESSAGE in combined
    assert "Traceback" not in combined
