from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ConfigPaths:
    config_dir: Path
    accounts_path: Path
    settings_path: Path
    legacy_ini_path: Path
    keys_path: Path
    yaml_path: Path | None
    two_file_mode: bool


def resolve_config_paths(config_dir: Path | None = None) -> ConfigPaths:
    default_config_dir = Path(__file__).resolve().parents[1] / "config"
    selected_dir = config_dir if config_dir is not None else default_config_dir

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


__all__ = ["ConfigPaths", "resolve_config_paths"]
