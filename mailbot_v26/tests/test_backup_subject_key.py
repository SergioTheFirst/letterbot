"""WP3a step 7: the subject-hash key travels with the database.

Backups carry ``<db>.subject.key`` as member data/mailbot.sqlite.subject.key
(subject hashes in restored events stay comparable); restore writes it next
to the resolved database (allowlisted, validated, atomic, same lock check);
a backup without the key leaves the current key alone; the support export
never contains it.
"""

from __future__ import annotations

import io
import sqlite3
import zipfile
from contextlib import closing
from pathlib import Path

import pytest

from mailbot_v26.storage.privacy_key import (
    ensure_subject_key,
    load_subject_key,
    subject_key_path,
)
from mailbot_v26.tools import backup, restore
from mailbot_v26.web_observability.doctor_export import build_diagnostics_zip

KEY_MEMBER = "data/mailbot.sqlite.subject.key"


@pytest.fixture(autouse=True)
def _no_tasklist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(restore, "_IS_WINDOWS", False)


def _make_db(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS notes (body TEXT)")
        conn.commit()
    return path


def _app(tmp_path: Path, *, db_setting: str | None = None) -> tuple[Path, Path]:
    root = tmp_path / "app"
    config_dir = root / "mailbot_v26" / "config"
    config_dir.mkdir(parents=True)
    if db_setting is not None:
        (config_dir / "settings.ini").write_text(
            f"[storage]\ndb_path = {db_setting}\n", encoding="utf-8"
        )
    db_path = backup.resolve_app_db_path(root)
    return root, _make_db(db_path)


def _key_bytes(db_path: Path) -> bytes:
    return subject_key_path(db_path).read_bytes()


def test_backup_carries_the_subject_key_as_a_fixed_member(tmp_path: Path) -> None:
    root, db_path = _app(tmp_path, db_setting="data/custom.sqlite")
    assert ensure_subject_key(db_path) is not None

    result = backup.create_backup(base_dir=root, retention=2)

    assert KEY_MEMBER in result.files_included
    with zipfile.ZipFile(result.archive_path) as archive:
        assert archive.read(KEY_MEMBER) == _key_bytes(db_path)
        assert "data/custom.sqlite.subject.key" not in archive.namelist()


def test_backup_without_a_key_file_has_no_key_member(tmp_path: Path) -> None:
    root, _db_path = _app(tmp_path)

    result = backup.create_backup(base_dir=root, retention=2)

    assert KEY_MEMBER not in result.files_included


def test_restore_writes_the_key_next_to_the_resolved_database(tmp_path: Path) -> None:
    root, db_path = _app(tmp_path, db_setting="data/custom.sqlite")
    original = ensure_subject_key(db_path)
    result = backup.create_backup(base_dir=root, retention=2)
    key_path = subject_key_path(db_path)
    key_path.unlink()

    outcome = restore.restore_from_backup(result.archive_path, base_dir=root)

    assert KEY_MEMBER in outcome.restored_files
    assert load_subject_key(db_path) == original
    assert not list(key_path.parent.glob("*.restore-tmp"))
    # The member name is fixed; the key lands next to data/custom.sqlite only.
    assert not (root / KEY_MEMBER).exists()


def test_restore_without_a_key_member_leaves_the_current_key(tmp_path: Path) -> None:
    root, db_path = _app(tmp_path)
    result = backup.create_backup(base_dir=root, retention=2)
    current = ensure_subject_key(db_path)
    before = _key_bytes(db_path)

    outcome = restore.restore_from_backup(result.archive_path, base_dir=root)

    assert KEY_MEMBER not in outcome.restored_files
    assert _key_bytes(db_path) == before
    assert load_subject_key(db_path) == current


@pytest.mark.parametrize(
    "payload",
    [b"not-a-key\n", b"ab" * 31 + b"\n", b"zz" * 32 + b"\n"],
    ids=["text", "short", "not-hex"],
)
def test_restore_skips_an_invalid_key_member(tmp_path: Path, payload: bytes) -> None:
    root, db_path = _app(tmp_path)
    current = ensure_subject_key(db_path)
    archive_path = tmp_path / "in.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(KEY_MEMBER, payload)

    outcome = restore.restore_from_backup(archive_path, base_dir=root)

    assert KEY_MEMBER in outcome.skipped_files
    assert any(KEY_MEMBER in note and "key" in note for note in outcome.warnings)
    assert load_subject_key(db_path) == current


def test_restore_skips_an_oversized_key_member(tmp_path: Path) -> None:
    root, db_path = _app(tmp_path)
    archive_path = tmp_path / "in.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(KEY_MEMBER, b"a" * 10_000)

    outcome = restore.restore_from_backup(archive_path, base_dir=root)

    assert KEY_MEMBER in outcome.skipped_files
    assert not subject_key_path(db_path).exists()


def test_restore_refused_while_running_leaves_the_key(tmp_path: Path) -> None:
    from mailbot_v26.system.instance_lock import acquire_instance_lock

    root, db_path = _app(tmp_path)
    ensure_subject_key(db_path)
    result = backup.create_backup(base_dir=root, retention=2)
    subject_key_path(db_path).unlink()

    holder = acquire_instance_lock(db_path)
    assert holder is not None
    try:
        with pytest.raises(restore.RestoreRefusedError):
            restore.restore_from_backup(result.archive_path, base_dir=root)
    finally:
        holder.release()
    assert not subject_key_path(db_path).exists()


def test_support_export_never_contains_the_subject_key(tmp_path: Path) -> None:
    db_path = _make_db(tmp_path / "data" / "mailbot.sqlite")
    key = ensure_subject_key(db_path)
    assert key is not None
    config_path = tmp_path / "config.yaml"
    config_path.write_text("app_name: test\n", encoding="utf-8")
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("2026-10-20 INFO started\n", encoding="utf-8")

    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=db_path,
        dist_root=tmp_path,
    )

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        contents = b"".join(archive.read(name) for name in names)
    assert not any("subject.key" in name for name in names)
    assert key.hex().encode("ascii") not in contents
    assert key.hex().upper().encode("ascii") not in contents
