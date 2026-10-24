from __future__ import annotations

import os
import sqlite3
import subprocess
import zipfile
from contextlib import closing
from pathlib import Path, PureWindowsPath

import pytest

from mailbot_v26.tools import backup, restore

_NO_TASKS = b"INFO: No tasks are running which match the specified criteria.\r\n"
_LETTERBOT_TASK = b'"Letterbot.exe","4242","Console","1","120 000 K"\r\n'


class _FakeTasklist:
    def __init__(self, stdout: bytes = _NO_TASKS, returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.calls: list[list[str]] = []

    def __call__(self, command, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append(list(command))
        return subprocess.CompletedProcess(
            command, self.returncode, stdout=self.stdout, stderr=b""
        )


@pytest.fixture(autouse=True)
def fake_tasklist(monkeypatch: pytest.MonkeyPatch) -> _FakeTasklist:
    fake = _FakeTasklist()
    monkeypatch.setattr(restore, "_IS_WINDOWS", True, raising=False)
    monkeypatch.setattr(subprocess, "run", fake)
    return fake


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_db(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS notes (body TEXT)")
        conn.executemany("INSERT INTO notes (body) VALUES (?)", [(r,) for r in rows])
        conn.commit()


def _db_rows(path: Path) -> list[str]:
    with closing(sqlite3.connect(path)) as conn:
        return [row[0] for row in conn.execute("SELECT body FROM notes ORDER BY rowid")]


def _db_path(base: Path) -> Path:
    return base / "data" / "mailbot.sqlite"


def test_backup_db_snapshot_includes_rows_still_in_wal(tmp_path: Path) -> None:
    db_path = _db_path(tmp_path)
    _make_db(db_path, ["checkpointed"])
    writer = sqlite3.connect(db_path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("INSERT INTO notes (body) VALUES ('only-in-wal')")
        writer.commit()
        assert Path(f"{db_path}-wal").stat().st_size > 0

        result = backup.create_backup(base_dir=tmp_path, retention=2)
    finally:
        writer.close()

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(result.archive_path) as archive:
        archive.extract("data/mailbot.sqlite", extracted)
        assert "data/mailbot.sqlite-wal" not in archive.namelist()
    assert _db_rows(extracted / "data" / "mailbot.sqlite") == [
        "checkpointed",
        "only-in-wal",
    ]


def test_restore_never_writes_masked_config_over_real_file(tmp_path: Path) -> None:
    config_dir = tmp_path / "mailbot_v26" / "config"
    accounts = config_dir / "accounts.ini"
    config_ini = config_dir / "config.ini"
    _write(accounts, "[acc]\nlogin=user@example.com\npassword=supersecret\n")
    _write(config_ini, "[general]\nadmin_chat_id=123\n")

    result = backup.create_backup(base_dir=tmp_path, retention=2)
    with zipfile.ZipFile(result.archive_path) as archive:
        assert "***" in archive.read("mailbot_v26/config/accounts.ini").decode("utf-8")

    _write(accounts, "[acc]\nlogin=user@example.com\npassword=newsecret\n")
    _write(config_ini, "[general]\nadmin_chat_id=999\n")

    outcome = restore.restore_from_backup(result.archive_path, base_dir=tmp_path)

    assert "password=newsecret" in accounts.read_text(encoding="utf-8")
    assert "***" not in accounts.read_text(encoding="utf-8")
    assert "mailbot_v26/config/accounts.ini" in outcome.skipped_files
    assert "mailbot_v26/config/accounts.ini" not in outcome.restored_files
    assert any(
        "accounts.ini" in note and "masked" in note for note in outcome.warnings
    )
    # A config without masked values is still restored.
    assert "admin_chat_id = 123" in config_ini.read_text(encoding="utf-8")
    assert "mailbot_v26/config/config.ini" in outcome.restored_files


def test_restore_does_not_create_config_with_masked_secrets(tmp_path: Path) -> None:
    accounts = tmp_path / "mailbot_v26" / "config" / "accounts.ini"
    _write(accounts, "[acc]\npassword=supersecret\n")
    result = backup.create_backup(base_dir=tmp_path, retention=2)
    accounts.unlink()

    outcome = restore.restore_from_backup(result.archive_path, base_dir=tmp_path)

    assert not accounts.exists()
    assert "mailbot_v26/config/accounts.ini" in outcome.skipped_files


def test_restore_refuses_while_letterbot_running(
    tmp_path: Path, fake_tasklist: _FakeTasklist
) -> None:
    _write(tmp_path / "state.json", '{"state": 1}')
    result = backup.create_backup(base_dir=tmp_path, retention=2)
    _write(tmp_path / "state.json", '{"state": 2}')
    fake_tasklist.stdout = _LETTERBOT_TASK

    with pytest.raises(restore.RestoreRefusedError, match="running"):
        restore.restore_from_backup(result.archive_path, base_dir=tmp_path)

    assert '"state": 2' in (tmp_path / "state.json").read_text(encoding="utf-8")
    command = fake_tasklist.calls[-1]
    # Windows path semantics on every CI OS (the check itself only runs on Windows).
    executable = PureWindowsPath(command[0])
    assert executable.is_absolute()
    assert executable.name.lower() == "tasklist.exe"
    assert executable.parent.name.lower() == "system32"
    assert "IMAGENAME eq Letterbot.exe" in command


@pytest.mark.parametrize("failure", ["oserror", "exit_code"])
def test_restore_fails_closed_when_running_check_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    _write(tmp_path / "state.json", '{"state": 1}')
    result = backup.create_backup(base_dir=tmp_path, retention=2)
    _write(tmp_path / "state.json", '{"state": 2}')

    if failure == "oserror":

        def _broken(command, **kwargs):  # noqa: ANN001, ANN003
            raise OSError("tasklist missing")

        monkeypatch.setattr(subprocess, "run", _broken)
    else:
        monkeypatch.setattr(subprocess, "run", _FakeTasklist(b"", 1))

    with pytest.raises(restore.RestoreRefusedError):
        restore.restore_from_backup(result.archive_path, base_dir=tmp_path)
    assert '"state": 2' in (tmp_path / "state.json").read_text(encoding="utf-8")


def test_run_restore_refuses_before_safety_backup_when_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_tasklist: _FakeTasklist
) -> None:
    archive_path = tmp_path / "in.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("state.json", '{"state": 1}')
    fake_tasklist.stdout = _LETTERBOT_TASK
    monkeypatch.setattr(restore, "_repo_root", lambda: tmp_path)

    def _no_prompt(prompt: str = "") -> str:
        raise AssertionError("must refuse before asking for confirmation")

    monkeypatch.setattr("builtins.input", _no_prompt)

    with pytest.raises(SystemExit, match="running"):
        restore.run_restore(str(archive_path))
    assert not (tmp_path / "backups").exists()


def test_run_restore_keeps_the_archive_it_restores_when_backups_are_full(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 14 archives (the retention): the safety backup must not prune the
    # oldest one, which is the archive being restored.
    backups_dir = tmp_path / "backups"
    backups_dir.mkdir()
    source = backups_dir / "backup_20200101_000000.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("state.json", '{"state": 1}')
    for index in range(1, backup.DEFAULT_RETENTION):
        with zipfile.ZipFile(backups_dir / f"backup_20200102_{index:06d}.zip", "w") as archive:
            archive.writestr("state.json", "{}")
    stamp = source.stat().st_mtime - 3600
    for offset, path in enumerate(sorted(backups_dir.glob("backup_*.zip"))):
        os.utime(path, (stamp + offset, stamp + offset))
    monkeypatch.setattr(restore, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(backup, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr("builtins.input", lambda prompt="": "YES")

    restore.run_restore(str(source))

    assert source.exists()
    assert (tmp_path / "state.json").read_text(encoding="utf-8") == '{"state": 1}'


def test_restore_replaces_db_and_removes_stale_wal_shm(tmp_path: Path) -> None:
    db_path = _db_path(tmp_path)
    _make_db(db_path, ["from-backup"])
    result = backup.create_backup(base_dir=tmp_path, retention=2)

    _make_db(db_path, ["added-later"])
    wal = Path(f"{db_path}-wal")
    shm = Path(f"{db_path}-shm")
    wal.write_bytes(b"stale wal from the replaced database")
    shm.write_bytes(b"stale shm")

    outcome = restore.restore_from_backup(result.archive_path, base_dir=tmp_path)

    assert "data/mailbot.sqlite" in outcome.restored_files
    assert not wal.exists()
    assert not shm.exists()
    assert _db_rows(db_path) == ["from-backup"]
    assert not list(db_path.parent.glob("*.restore-tmp"))


def test_restore_skips_member_over_size_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_path = tmp_path / "in.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("state.json", "x" * 4096)
        zf.writestr("mailbot_v26/runtime_flags.json", "{}")
    _write(tmp_path / "state.json", '{"state": "current"}')
    monkeypatch.setattr(restore, "_MAX_MEMBER_BYTES", 1024)

    outcome = restore.restore_from_backup(archive_path, base_dir=tmp_path)

    assert "state.json" in outcome.skipped_files
    assert any("state.json" in note and "limit" in note for note in outcome.warnings)
    assert (tmp_path / "state.json").read_text(encoding="utf-8") == (
        '{"state": "current"}'
    )
    assert "mailbot_v26/runtime_flags.json" in outcome.restored_files

