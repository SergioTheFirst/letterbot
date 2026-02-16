from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("yaml")

from mailbot_v26 import start


def test_resolve_config_path_prefers_module_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_dir = tmp_path / "mailbot_v26"
    module_dir.mkdir()
    module_config = module_dir / "config.yaml"
    root_config = tmp_path / "config.yaml"
    module_config.write_text("{}", encoding="utf-8")
    root_config.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(start, "CURRENT_DIR", module_dir)

    resolved = start._resolve_config_path(None)

    assert resolved == module_config


def test_resolve_config_path_falls_back_to_repo_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_dir = tmp_path / "mailbot_v26"
    module_dir.mkdir()
    root_config = tmp_path / "config.yaml"
    root_config.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(start, "CURRENT_DIR", module_dir)

    resolved = start._resolve_config_path(None)

    assert resolved == root_config


def test_load_yaml_config_or_exit_invalid_config_logs_and_exits(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        start._load_yaml_config_or_exit(config_path)

    assert exc.value.code == 1
