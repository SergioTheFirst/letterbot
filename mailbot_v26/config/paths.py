from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys


@dataclass(frozen=True, slots=True)
class ConfigPaths:
    config_dir: Path
    accounts_path: Path
    settings_path: Path
    legacy_ini_path: Path
    keys_path: Path
    yaml_path: Path | None
    two_file_mode: bool


def app_root() -> Path:
    """Folder that contains mailbot_v26/: the exe folder when frozen, else the repo root."""
    if bool(getattr(sys, "frozen", False)):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def default_config_dir() -> Path:
    """Canonical config folder (<app_root>/mailbot_v26/config) in source and frozen mode."""
    return app_root() / "mailbot_v26" / "config"


def runtime_log_path() -> Path:
    """mailbot.log written by the worker (package folder; <exe>/_internal/mailbot_v26 when frozen)."""
    return Path(__file__).resolve().parents[1] / "mailbot.log"


def resolve_config_paths(config_dir: Path | None = None) -> ConfigPaths:
    selected_dir = config_dir if config_dir is not None else default_config_dir()

    yaml_candidates: list[Path]
    if selected_dir.is_file():
        yaml_candidates = [selected_dir]
        selected_dir = selected_dir.parent
    else:
        # Never read config.yaml from repo root implicitly.
        # Root YAML is only allowed when passed explicitly as --config-dir <path-to-yaml>.
        yaml_candidates = [selected_dir / "config.yaml"]

    yaml_path = next((path for path in yaml_candidates if path.exists()), None)

    accounts_path = selected_dir / "accounts.ini"
    return ConfigPaths(
        config_dir=selected_dir,
        accounts_path=accounts_path,
        settings_path=selected_dir / "settings.ini",
        legacy_ini_path=selected_dir / "config.ini",
        keys_path=selected_dir / "keys.ini",
        yaml_path=yaml_path,
        two_file_mode=accounts_path.exists(),
    )

__all__ = [
    "ConfigPaths",
    "app_root",
    "default_config_dir",
    "resolve_config_paths",
    "runtime_log_path",
]
