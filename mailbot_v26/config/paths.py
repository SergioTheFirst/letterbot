from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging
import os
import sys

_LOGGER = logging.getLogger(__name__)

# storage.db_path when settings.ini has none (relative to app_root()).
DEFAULT_DB_RELATIVE_PATH = Path("data") / "mailbot.sqlite"
# PyInstaller one-folder builds keep the package in <exe>/_internal; frozen
# builds before B25 resolved a relative db_path against that folder.
FROZEN_CONTENTS_DIR = "_internal"
# (legacy, new) locations already reported in this process.
_DB_LOCATION_WARNED: set[tuple[str, str]] = set()


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


def resolve_db_path(
    configured: str | os.PathLike[str] | None = None, *, root: Path | None = None
) -> Path:
    """The database file for a storage.db_path value (worker, web and tools).

    An absolute path is used as given. A relative or missing one (default
    data/mailbot.sqlite) resolves against root, by default app_root(): the
    exe folder when frozen, the repo root otherwise.

    Frozen builds before B25 put it in <exe>/_internal instead. While that
    legacy file exists it stays in use and a warning names both locations:
    a live WAL database is never moved, and a later start can never switch
    to a new, empty file at the other location.
    """
    text = os.fspath(configured).strip() if configured is not None else ""
    relative = Path(text) if text else DEFAULT_DB_RELATIVE_PATH
    if relative.is_absolute():
        return relative
    base = Path(root) if root is not None else app_root()
    resolved = (base / relative).resolve()
    if not bool(getattr(sys, "frozen", False)):
        return resolved
    legacy = (base / FROZEN_CONTENTS_DIR / relative).resolve()
    if legacy == resolved or not legacy.exists():
        return resolved
    _warn_legacy_db_location(legacy=legacy, new=resolved)
    return legacy


def _warn_legacy_db_location(*, legacy: Path, new: Path) -> None:
    key = (str(legacy), str(new))
    if key in _DB_LOCATION_WARNED:
        return
    _DB_LOCATION_WARNED.add(key)
    if new.exists():
        _LOGGER.warning(
            "database_location_conflict using=%s ignored=%s: both files exist; "
            "Letterbot keeps the database it used before. Stop Letterbot and "
            "remove or rename the one you do not need.",
            legacy,
            new,
        )
        return
    _LOGGER.warning(
        "database_legacy_location path=%s new_location=%s: older builds kept the "
        "database inside %s and it stays in use. To move it, stop Letterbot and "
        "move the files whose names start with %s to %s.",
        legacy,
        new,
        FROZEN_CONTENTS_DIR,
        legacy.name,
        new.parent,
    )


def import_time_db_path() -> Path:
    """Database the pipeline's module-level singletons use until start.main configures the real one.

    LETTERBOT_DEFAULT_DB_PATH overrides it; the test suite points it at a temp file
    so importing the pipeline never opens a real database.
    """
    override = os.environ.get("LETTERBOT_DEFAULT_DB_PATH", "").strip()
    return Path(override) if override else Path("database.sqlite")


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
    "DEFAULT_DB_RELATIVE_PATH",
    "app_root",
    "default_config_dir",
    "import_time_db_path",
    "resolve_config_paths",
    "resolve_db_path",
    "runtime_log_path",
]
