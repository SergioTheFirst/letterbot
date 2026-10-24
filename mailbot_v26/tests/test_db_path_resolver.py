"""B25: one database path resolver for the worker, backup, restore, export, doctor.

A relative (or missing) storage.db_path resolves against app_root(): the exe
folder when frozen, the repo root in source mode. Frozen builds before this
fix resolved it against <exe>/_internal; while that legacy file exists it
stays in use (a live WAL database is never moved), so no start can switch to
a new empty file. backup always stores the database as data/mailbot.sqlite
and restore writes that member to the resolved path.
"""

from __future__ import annotations

import logging
import sqlite3
import subprocess
import zipfile
from contextlib import closing
from pathlib import Path

import pytest

from mailbot_v26 import config_loader
from mailbot_v26.config import paths
from mailbot_v26.config.paths import app_root, resolve_db_path
from mailbot_v26.config_loader import load_storage_config
from mailbot_v26.tools import backup, export_data, restore


@pytest.fixture(autouse=True)
def _fresh_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(paths, "_DB_LOCATION_WARNED", set())
    monkeypatch.setattr(restore, "_IS_WINDOWS", False)


def _frozen_dist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    dist_dir = tmp_path / "dist" / "Letterbot"
    (dist_dir / "mailbot_v26" / "config").mkdir(parents=True)
    monkeypatch.setattr("sys.frozen", True, raising=False)
    monkeypatch.setattr("sys.executable", str(dist_dir / "Letterbot.exe"))
    return dist_dir


def _config_dir(root: Path) -> Path:
    return root / "mailbot_v26" / "config"


def _make_db(path: Path, marker: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS notes (body TEXT)")
        conn.execute("DELETE FROM notes")
        conn.execute("INSERT INTO notes (body) VALUES (?)", (marker,))
        conn.commit()
    return path


def _marker(path: Path) -> str:
    with closing(sqlite3.connect(path)) as conn:
        return str(conn.execute("SELECT body FROM notes").fetchone()[0])


# --- resolver -----------------------------------------------------------------


def test_source_mode_default_matches_the_previous_package_parent_rule(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delattr("sys.frozen", raising=False)
    previous_root = Path(config_loader.__file__).resolve().parents[1]

    assert app_root() == previous_root
    assert resolve_db_path(None) == previous_root / "data" / "mailbot.sqlite"
    assert load_storage_config(tmp_path).db_path == previous_root / "data" / "mailbot.sqlite"
    (tmp_path / "settings.ini").write_text(
        "[storage]\ndb_path = data/custom.sqlite\n", encoding="utf-8"
    )
    assert load_storage_config(tmp_path).db_path == (
        previous_root / "data" / "custom.sqlite"
    ).resolve()


def test_absolute_db_path_is_used_as_given(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere" / "letterbot.sqlite"
    (tmp_path / "settings.ini").write_text(
        f"[storage]\ndb_path = {target}\n", encoding="utf-8"
    )

    assert load_storage_config(tmp_path).db_path == target


def test_frozen_db_path_resolves_next_to_the_exe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)

    resolved = load_storage_config(_config_dir(dist_dir)).db_path

    assert resolved == dist_dir / "data" / "mailbot.sqlite"
    assert "_internal" not in resolved.parts


def test_frozen_legacy_internal_database_stays_in_use(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    legacy = _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "legacy")
    caplog.set_level(logging.WARNING)

    first = load_storage_config(_config_dir(dist_dir)).db_path
    second = resolve_db_path("data/mailbot.sqlite")

    assert first == second == legacy
    assert caplog.text.count("database_legacy_location") == 1
    assert not (dist_dir / "data" / "mailbot.sqlite").exists()


def test_frozen_legacy_database_wins_over_a_new_file_appearing_later(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    legacy = _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "legacy")
    _make_db(dist_dir / "data" / "mailbot.sqlite", "new-empty")
    caplog.set_level(logging.WARNING)

    assert resolve_db_path(None) == legacy
    assert "database_location_conflict" in caplog.text


def test_frozen_relative_custom_path_uses_its_legacy_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    (_config_dir(dist_dir) / "settings.ini").write_text(
        "[storage]\ndb_path = data/custom.sqlite\n", encoding="utf-8"
    )
    assert load_storage_config(_config_dir(dist_dir)).db_path == (
        dist_dir / "data" / "custom.sqlite"
    )
    legacy = _make_db(dist_dir / "_internal" / "data" / "custom.sqlite", "legacy")

    assert load_storage_config(_config_dir(dist_dir)).db_path == legacy


def test_frozen_absolute_db_path_has_no_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "legacy")
    target = tmp_path / "data" / "mailbot.sqlite"

    assert resolve_db_path(str(target)) == target


# --- tools use the same resolver -------------------------------------------


def test_frozen_backup_stores_legacy_database_as_data_member(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "legacy")

    result = backup.create_backup(retention=2)

    assert result.archive_path.parent == dist_dir / "backups"
    assert "data/mailbot.sqlite" in result.files_included
    assert not any("_internal" in name for name in result.files_included)
    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(result.archive_path) as archive:
        archive.extract("data/mailbot.sqlite", extracted)
    assert _marker(extracted / "data" / "mailbot.sqlite") == "legacy"


def test_backup_stores_a_database_outside_the_app_folder(tmp_path: Path) -> None:
    root = tmp_path / "app"
    outside = _make_db(tmp_path / "elsewhere" / "letterbot.sqlite", "outside")
    _config_dir(root).mkdir(parents=True)
    (_config_dir(root) / "settings.ini").write_text(
        f"[storage]\ndb_path = {outside}\n", encoding="utf-8"
    )

    result = backup.create_backup(base_dir=root, retention=2)

    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(result.archive_path) as archive:
        assert archive.namelist().count("data/mailbot.sqlite") == 1
        archive.extract("data/mailbot.sqlite", extracted)
    assert _marker(extracted / "data" / "mailbot.sqlite") == "outside"


def test_frozen_restore_writes_the_member_to_the_resolved_legacy_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    legacy = _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "before")
    result = backup.create_backup(retention=2)
    _make_db(legacy, "changed-later")

    outcome = restore.restore_from_backup(result.archive_path)

    assert "data/mailbot.sqlite" in outcome.restored_files
    assert _marker(legacy) == "before"
    assert not (dist_dir / "data" / "mailbot.sqlite").exists()


def test_frozen_restore_round_trips_the_database_next_to_the_exe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    db_path = _make_db(dist_dir / "data" / "mailbot.sqlite", "before")
    result = backup.create_backup(retention=2)
    _make_db(db_path, "changed-later")

    outcome = restore.restore_from_backup(result.archive_path)

    assert "data/mailbot.sqlite" in outcome.restored_files
    assert _marker(db_path) == "before"
    assert not (dist_dir / "_internal").exists()


def test_restore_accepts_a_backup_made_by_an_older_frozen_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Older frozen builds stored the database as _internal/data/mailbot.sqlite,
    # which restore skipped; it now goes to the resolved database.
    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    old_snapshot = _make_db(tmp_path / "old" / "mailbot.sqlite", "from-old-build")
    archive = tmp_path / "backup_old.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(old_snapshot, "_internal/data/mailbot.sqlite")

    outcome = restore.restore_from_backup(archive)

    assert outcome.restored_files == ("_internal/data/mailbot.sqlite",)
    assert _marker(dist_dir / "data" / "mailbot.sqlite") == "from-old-build"
    assert not (dist_dir / "_internal").exists()


def test_frozen_restore_checks_the_lock_of_the_resolved_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from mailbot_v26.system.instance_lock import acquire_instance_lock

    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    legacy = _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "before")
    result = backup.create_backup(retention=2)
    _make_db(legacy, "worker-is-writing")

    holder = acquire_instance_lock(legacy)
    assert holder is not None
    try:
        with pytest.raises(restore.RestoreRefusedError, match="running"):
            restore.restore_from_backup(result.archive_path)
    finally:
        holder.release()
    assert _marker(legacy) == "worker-is-writing"


def test_frozen_export_and_doctor_use_the_resolved_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from mailbot_v26 import doctor

    dist_dir = _frozen_dist(monkeypatch, tmp_path)
    legacy = _make_db(dist_dir / "_internal" / "data" / "mailbot.sqlite", "legacy")

    assert export_data._resolve_db_path(dist_dir) == legacy
    _entries, data = doctor._check_config_files(_config_dir(dist_dir))
    assert data["db_path"] == legacy


# --- restore's running-process check ignores the restore itself ------------


def test_restore_running_check_ignores_its_own_letterbot_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    own_pid = 4242
    rows = f'"Letterbot.exe","{own_pid}","Console","1","120 000 K"\r\n'.encode()

    def _tasklist(command, **_kwargs):
        return subprocess.CompletedProcess(command, 0, stdout=rows, stderr=b"")

    monkeypatch.setattr(restore, "_IS_WINDOWS", True)
    monkeypatch.setattr(restore.os, "getpid", lambda: own_pid)
    monkeypatch.setattr(subprocess, "run", _tasklist)

    restore.ensure_letterbot_stopped(db_path=tmp_path / "data" / "mailbot.sqlite")

    other = rows + b'"Letterbot.exe","5151","Console","1","90 000 K"\r\n'
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(
            command, 0, stdout=other, stderr=b""
        ),
    )
    with pytest.raises(restore.RestoreRefusedError, match="running"):
        restore.ensure_letterbot_stopped(db_path=tmp_path / "data" / "mailbot.sqlite")
