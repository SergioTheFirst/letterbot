from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.tools.backup import create_backup


@dataclass(frozen=True)
class RestoreResult:
    restored_files: tuple[str, ...]
    skipped_files: tuple[str, ...]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_safe_member(member: str) -> bool:
    path = Path(member)
    if path.is_absolute():
        return False
    if ".." in path.parts:
        return False
    return True


def _is_allowed_path(member: str) -> bool:
    if member in {"state.json", "data/mailbot.sqlite"}:
        return True
    if member == "mailbot_v26/runtime_flags.json":
        return True
    if member == "mailbot_v26/data/runtime_health.json":
        return True
    if member.startswith("mailbot_v26/config/") and member.endswith(".ini"):
        return True
    return False


def restore_from_backup(
    archive_path: Path, base_dir: Path | None = None
) -> RestoreResult:
    root = base_dir or _repo_root()
    restored: list[str] = []
    skipped: list[str] = []

    with zipfile.ZipFile(archive_path, "r") as archive:
        for member in archive.namelist():
            if not _is_safe_member(member) or not _is_allowed_path(member):
                skipped.append(member)
                continue
            destination = root / member
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, destination.open("wb") as target:
                target.write(source.read())
            restored.append(member)

    return RestoreResult(restored_files=tuple(restored), skipped_files=tuple(skipped))


def run_restore(archive_path: str) -> None:
    path = Path(archive_path)
    if not path.exists():
        raise SystemExit(f"Backup archive not found: {path}")

    print(f"[WARN] Restore will overwrite local data using: {path}")
    confirm = input("Type YES to continue: ").strip()
    if confirm != "YES":
        raise SystemExit("Restore canceled by user.")

    create_backup()
    result = restore_from_backup(path)

    if result.restored_files:
        print("[OK] Restored files:")
        for name in result.restored_files:
            print(f" - {name}")
    if result.skipped_files:
        print("[WARN] Skipped files:")
        for name in result.skipped_files:
            print(f" - {name}")


__all__ = ["RestoreResult", "restore_from_backup", "run_restore"]
