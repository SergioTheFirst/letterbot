from __future__ import annotations

import io
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from mailbot_v26.config_loader import ConfigError, load_storage_config
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults

DEFAULT_RETENTION = 14


@dataclass(frozen=True)
class BackupResult:
    archive_path: Path
    files_included: tuple[str, ...]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_db_path(base_dir: Path) -> Path:
    config_dir = base_dir / "mailbot_v26" / "config"
    try:
        resolved = load_storage_config(config_dir).db_path
        try:
            resolved.relative_to(base_dir)
        except ValueError:
            return base_dir / "data" / "mailbot.sqlite"
        return resolved
    except ConfigError:
        return base_dir / "data" / "mailbot.sqlite"


def _default_paths(base_dir: Path) -> list[Path]:
    return [
        _resolve_db_path(base_dir),
        base_dir / "state.json",
        base_dir / "mailbot_v26" / "runtime_flags.json",
        base_dir / "mailbot_v26" / "data" / "runtime_health.json",
    ]


def _iter_config_files(base_dir: Path) -> Iterable[Path]:
    config_dir = base_dir / "mailbot_v26" / "config"
    if not config_dir.exists():
        return []
    return sorted(path for path in config_dir.glob("*.ini") if path.is_file())


def _should_mask(option: str) -> bool:
    lowered = option.lower()
    return any(token in lowered for token in ("password", "token", "secret", "key"))


def _mask_config(path: Path) -> str:
    parser = read_user_ini_with_defaults(path, scope_label="backup config")
    for section in parser.sections():
        for option in parser.options(section):
            if _should_mask(option):
                parser.set(section, option, "***")
    buffer = io.StringIO()
    parser.write(buffer)
    return buffer.getvalue()


def _collect_backup_files(base_dir: Path, temp_dir: Path) -> list[tuple[Path, str]]:
    entries: list[tuple[Path, str]] = []

    for path in _default_paths(base_dir):
        if path.exists():
            entries.append((path, path.relative_to(base_dir).as_posix()))

    for path in _iter_config_files(base_dir):
        sanitized = _mask_config(path)
        target = temp_dir / path.name
        target.write_text(sanitized, encoding="utf-8")
        entries.append((target, path.relative_to(base_dir).as_posix()))

    return sorted(entries, key=lambda item: item[1])


def _prune_backups(backups_dir: Path, retention: int) -> None:
    if retention <= 0:
        return
    backups = sorted(backups_dir.glob("backup_*.zip"), key=lambda p: p.stat().st_mtime)
    excess = backups[:-retention]
    for path in excess:
        path.unlink(missing_ok=True)


def create_backup(
    base_dir: Path | None = None, *, retention: int = DEFAULT_RETENTION
) -> BackupResult:
    root = base_dir or _repo_root()
    backups_dir = root / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = backups_dir / f"backup_{timestamp}.zip"

    with tempfile.TemporaryDirectory() as temp_root:
        temp_dir = Path(temp_root)
        entries = _collect_backup_files(root, temp_dir)
        with zipfile.ZipFile(
            archive_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            for source, arcname in entries:
                archive.write(source, arcname)

    _prune_backups(backups_dir, retention)
    return BackupResult(
        archive_path=archive_path, files_included=tuple(name for _, name in entries)
    )


def run_backup() -> None:
    result = create_backup()
    print(f"[OK] Backup created: {result.archive_path}")
    if result.files_included:
        print("[OK] Included files:")
        for name in result.files_included:
            print(f" - {name}")
    else:
        print("[WARN] No files were added to the backup archive.")


__all__ = ["BackupResult", "create_backup", "run_backup"]
