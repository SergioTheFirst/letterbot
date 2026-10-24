from __future__ import annotations

import configparser
import csv
import os
import re
import sqlite3
import subprocess
import zipfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from mailbot_v26.tools.backup import (
    DB_MEMBER,
    MASKED_VALUE,
    SUBJECT_KEY_MEMBER,
    create_backup,
    resolve_app_db_path,
)
from mailbot_v26.config.paths import app_root
from mailbot_v26.storage.privacy_key import SUBJECT_KEY_BYTES, subject_key_path
from mailbot_v26.storage.sqlite_uri import sqlite_readonly_uri
from mailbot_v26.system.instance_lock import InstanceLockError, is_instance_running

_IS_WINDOWS = os.name == "nt"
_DB_MEMBER = DB_MEMBER
# Frozen builds before B25 stored the database under this name; such a
# backup still restores it (when it has no data/mailbot.sqlite).
_LEGACY_FROZEN_DB_MEMBER = "_internal/data/mailbot.sqlite"
_CONFIG_PREFIX = "mailbot_v26/config/"
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")
_MAX_MEMBER_BYTES = 16 * 1024 * 1024
_MAX_DB_MEMBER_BYTES = 8 * 1024 * 1024 * 1024
_MAX_KEY_MEMBER_BYTES = 4096
_SUBJECT_KEY_RE = re.compile(rb"[0-9a-fA-F]{%d}" % (SUBJECT_KEY_BYTES * 2))
_COPY_CHUNK_BYTES = 1024 * 1024
_TASKLIST_TIMEOUT_SECONDS = 30


class RestoreRefusedError(RuntimeError):
    """Restore cannot run safely: Letterbot is running or that cannot be verified."""


@dataclass(frozen=True)
class RestoreResult:
    restored_files: tuple[str, ...]
    skipped_files: tuple[str, ...]
    warnings: tuple[str, ...] = ()


def _repo_root() -> Path:
    # Exe folder when frozen (config lives in <exe>\mailbot_v26\config), repo root otherwise.
    return app_root()


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


def _tasklist_path() -> Path:
    system_root = os.environ.get("SystemRoot") or r"C:\Windows"
    return Path(system_root) / "System32" / "tasklist.exe"


def ensure_letterbot_stopped(db_path: Path | None = None) -> None:
    """Raise RestoreRefusedError while Letterbot runs (fails closed if unknown).

    db_path: the database restore would replace; its worker instance lock is
    checked first (also catches a worker started from source), then tasklist.
    """
    if db_path is not None:
        try:
            running = is_instance_running(db_path)
        except InstanceLockError as exc:
            raise RestoreRefusedError(
                f"Could not verify that Letterbot is stopped ({exc}); restore not started."
            ) from exc
        if running:
            raise RestoreRefusedError(
                "Letterbot is running. Close Letterbot (worker and web UI) and run restore again."
            )
    if not _IS_WINDOWS:
        return
    command = [
        str(_tasklist_path()),
        "/FI",
        "IMAGENAME eq Letterbot.exe",
        "/FO",
        "CSV",
        "/NH",
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            timeout=_TASKLIST_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RestoreRefusedError(
            f"Could not verify that Letterbot is stopped ({exc}); restore not started."
        ) from exc
    if completed.returncode != 0:
        raise RestoreRefusedError(
            "Could not verify that Letterbot is stopped "
            f"(tasklist exit code {completed.returncode}); restore not started."
        )
    if _other_letterbot_pids(completed.stdout.decode("ascii", "replace")):
        raise RestoreRefusedError(
            "Letterbot is running. Close Letterbot (worker and web UI) and run restore again."
        )


def _other_letterbot_pids(tasklist_csv: str) -> list[str]:
    """PIDs of Letterbot.exe processes other than this one (Letterbot.exe restore)."""
    # CSV rows carry the quoted image name and PID; the "no tasks" notice is
    # localized and has no such row.
    own_pid = str(os.getpid())
    pids: list[str] = []
    for row in csv.reader(tasklist_csv.splitlines()):
        if len(row) < 2 or row[0].strip().lower() != "letterbot.exe":
            continue
        pid = row[1].strip()
        if pid != own_pid:
            pids.append(pid)
    return pids


def _config_skip_reason(data: bytes) -> str | None:
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read_string(data.decode("utf-8-sig"))
    except (UnicodeDecodeError, configparser.Error) as exc:
        return f"backup copy is not a readable INI file ({type(exc).__name__})"
    if not parser.sections():
        return "backup copy has no settings"
    for section in parser.values():
        if any(value.strip() == MASKED_VALUE for value in section.values()):
            return f"backup copy has masked secrets ({MASKED_VALUE})"
    return None


def _extract_to_temp(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo, destination: Path
) -> Path:
    # zipfile stops reading at the declared size (checked against the cap by the caller).
    temp_path = destination.with_name(f"{destination.name}.restore-tmp")
    try:
        with archive.open(info) as source, temp_path.open("wb") as target:
            while chunk := source.read(_COPY_CHUNK_BYTES):
                target.write(chunk)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def _db_member_refusal(path: Path) -> str | None:
    """Why the extracted backup database must not replace the current one."""
    # immutable=1: the private temp copy never changes, and SQLite creates
    # no -wal/-shm next to it.
    uri = f"{sqlite_readonly_uri(path)}&immutable=1"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(emails);")
            }
    except sqlite3.Error as exc:
        return (
            f"The database in this backup cannot be read ({type(exc).__name__}); "
            "nothing was restored. Use another backup archive."
        )
    if columns and "uid" not in columns:
        return (
            "The database in this backup has an emails table without the uid "
            "column (an older format Letterbot cannot run on); nothing was "
            "restored. Use a backup created by Letterbot's backup command."
        )
    return None


def _restore_subject_key(
    archive: zipfile.ZipFile, info: zipfile.ZipInfo, destination: Path
) -> str | None:
    """Write the subject-hash key atomically; a skip reason when it is unusable."""
    kept = "current key left unchanged" if destination.exists() else "not created"
    if info.file_size > _MAX_KEY_MEMBER_BYTES:
        return f"{info.file_size} bytes exceeds the {_MAX_KEY_MEMBER_BYTES} byte limit; {kept}"
    data = archive.read(info)
    if not _SUBJECT_KEY_RE.fullmatch(data.strip()):
        return f"backup copy is not a valid subject key; {kept}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f"{destination.name}.restore-tmp")
    try:
        fd = os.open(
            temp_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0),
            0o600,
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, destination)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return None


def _remove_sqlite_sidecars(db_path: Path) -> None:
    # A stale -wal/-shm from the replaced database would corrupt the restored one.
    for suffix in _SQLITE_SIDECARS:
        sidecar = db_path.with_name(f"{db_path.name}{suffix}")
        try:
            sidecar.unlink(missing_ok=True)
        except OSError as exc:
            raise RestoreRefusedError(
                f"Cannot remove {sidecar.name} ({exc}); close Letterbot and run restore again."
            ) from exc


def _restore_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    destination: Path,
    *,
    is_db: bool = False,
) -> str | None:
    """Write one allowed member; return a skip reason instead of writing when unsafe."""
    member = info.filename
    limit = _MAX_DB_MEMBER_BYTES if is_db else _MAX_MEMBER_BYTES
    if info.file_size > limit:
        return f"{info.file_size} bytes exceeds the {limit} byte limit"
    if member.startswith(_CONFIG_PREFIX):
        reason = _config_skip_reason(archive.read(info))
        if reason is not None:
            kept = "current file left unchanged" if destination.exists() else "not created"
            return f"{reason}; {kept}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _extract_to_temp(archive, info, destination)
    try:
        if is_db:
            refusal = _db_member_refusal(temp_path)
            if refusal is not None:
                raise RestoreRefusedError(refusal)
            _remove_sqlite_sidecars(destination)
        os.replace(temp_path, destination)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    return None


def restore_from_backup(
    archive_path: Path, base_dir: Path | None = None
) -> RestoreResult:
    root = base_dir or _repo_root()
    restored: list[str] = []
    skipped: list[str] = []
    warnings: list[str] = []

    # The database the worker uses, wherever it lives (B25): its lock is the
    # one to check and the data/mailbot.sqlite member is written there.
    db_path = resolve_app_db_path(root)
    ensure_letterbot_stopped(db_path=db_path)
    with zipfile.ZipFile(archive_path, "r") as archive:
        names = set(archive.namelist())
        db_member = _DB_MEMBER
        if _DB_MEMBER not in names and _LEGACY_FROZEN_DB_MEMBER in names:
            db_member = _LEGACY_FROZEN_DB_MEMBER
        # The database first: when it is refused no other file has changed.
        members = sorted(archive.infolist(), key=lambda info: info.filename != db_member)
        for info in members:
            member = info.filename
            if member == db_member:
                reason = _restore_member(archive, info, db_path, is_db=True)
            elif member == SUBJECT_KEY_MEMBER:
                # Next to the database it belongs to, wherever that lives.
                reason = _restore_subject_key(archive, info, subject_key_path(db_path))
            elif _is_safe_member(member) and _is_allowed_path(member):
                reason = _restore_member(archive, info, root / member)
            else:
                skipped.append(member)
                continue
            if reason is not None:
                skipped.append(member)
                warnings.append(f"{member}: {reason}")
                continue
            restored.append(member)

    return RestoreResult(
        restored_files=tuple(restored),
        skipped_files=tuple(skipped),
        warnings=tuple(warnings),
    )


def run_restore(archive_path: str) -> None:
    path = Path(archive_path)
    if not path.exists():
        raise SystemExit(f"Backup archive not found: {path}")
    try:
        ensure_letterbot_stopped(db_path=resolve_app_db_path(_repo_root()))
    except RestoreRefusedError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc

    print(f"[WARN] Restore will overwrite local data using: {path}")
    confirm = input("Type YES to continue: ").strip()
    if confirm != "YES":
        raise SystemExit("Restore canceled by user.")

    # No pruning here: with a full backups folder it would delete the oldest
    # archive, which may be the one being restored.
    safety = create_backup(retention=0)
    print(
        "[OK] Safety backup (passwords masked; it holds your mail database "
        f"and subject key, keep it on this computer): {safety.archive_path}"
    )
    try:
        result = restore_from_backup(path)
    except RestoreRefusedError as exc:
        raise SystemExit(f"[ERROR] {exc}") from exc

    if result.restored_files:
        print("[OK] Restored files:")
        for name in result.restored_files:
            print(f" - {name}")
    if result.skipped_files:
        print("[WARN] Skipped files:")
        for name in result.skipped_files:
            print(f" - {name}")
    for note in result.warnings:
        print(f"[WARN] {note}")


__all__ = [
    "RestoreRefusedError",
    "RestoreResult",
    "ensure_letterbot_stopped",
    "restore_from_backup",
    "run_restore",
]
