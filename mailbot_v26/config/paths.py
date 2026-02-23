from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ConfigPaths:
    config_dir: Path
    ini_path: Path
    yaml_path: Path | None


def resolve_config_paths(config_dir: Path | None = None) -> ConfigPaths:
    root_dir = Path(__file__).resolve().parents[2]
    default_config_dir = root_dir / "mailbot_v26" / "config"
    selected_dir = config_dir if config_dir is not None else default_config_dir

    yaml_candidates: list[Path]
    if selected_dir.is_file():
        yaml_candidates = [selected_dir]
        selected_dir = selected_dir.parent
    else:
        yaml_candidates = [
            root_dir / "config.yaml",
            selected_dir / "config.yaml",
        ]

    yaml_path = next((path for path in yaml_candidates if path.exists()), None)

    return ConfigPaths(
        config_dir=selected_dir,
        ini_path=selected_dir / "config.ini",
        yaml_path=yaml_path,
    )


__all__ = ["ConfigPaths", "resolve_config_paths"]
