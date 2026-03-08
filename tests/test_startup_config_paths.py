from __future__ import annotations

from pathlib import Path

from mailbot_v26.config.paths import resolve_config_paths


def test_resolve_config_paths_uses_config_dir_yaml_only(tmp_path: Path) -> None:
    config_dir = tmp_path / "mailbot_v26" / "config"
    config_dir.mkdir(parents=True)
    (tmp_path / "config.yaml").write_text("root: true", encoding="utf-8")
    (config_dir / "config.yaml").write_text("local: true", encoding="utf-8")

    resolved = resolve_config_paths(config_dir)

    assert resolved.yaml_path == config_dir / "config.yaml"


def test_resolve_config_paths_does_not_read_repo_root_yaml_implicitly(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "mailbot_v26" / "config"
    config_dir.mkdir(parents=True)
    (tmp_path / "config.yaml").write_text("root: true", encoding="utf-8")

    resolved = resolve_config_paths(config_dir)

    assert resolved.yaml_path is None


def test_resolve_config_paths_allows_explicit_yaml_path(tmp_path: Path) -> None:
    explicit = tmp_path / "config.yaml"
    explicit.write_text("x: 1", encoding="utf-8")

    resolved = resolve_config_paths(explicit)

    assert resolved.yaml_path == explicit
    assert resolved.config_dir == tmp_path
