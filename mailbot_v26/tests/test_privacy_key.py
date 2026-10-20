"""WP3a step 1: the local subject-hash key next to the database."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from pathlib import Path

import pytest

from mailbot_v26.storage import privacy_key
from mailbot_v26.storage.privacy_key import (
    ensure_subject_key,
    load_subject_key,
    subject_key_id,
    subject_key_path,
)


def _db(tmp_path: Path) -> Path:
    return tmp_path / "data" / "database.sqlite"


def _log_text(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def test_key_file_sits_next_to_the_database(tmp_path: Path) -> None:
    db_path = _db(tmp_path)

    assert subject_key_path(db_path) == db_path.parent / "database.sqlite.subject.key"
    assert subject_key_path(str(db_path)) == subject_key_path(db_path)


def test_key_is_created_once_and_stays_stable(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _db(tmp_path)
    caplog.set_level(logging.INFO, logger="mailbot")

    key = ensure_subject_key(db_path)

    assert key is not None and len(key) == privacy_key.SUBJECT_KEY_BYTES == 32
    path = subject_key_path(db_path)
    content = path.read_bytes()
    assert re.fullmatch(rb"[0-9a-f]{64}\n", content)
    assert bytes.fromhex(content.decode("ascii").strip()) == key
    assert "subject_hash_key_created" in _log_text(caplog)

    assert ensure_subject_key(db_path) == key
    assert ensure_subject_key(db_path, expected_key_id=subject_key_id(key)) == key
    assert load_subject_key(db_path) == key
    assert path.read_bytes() == content
    assert sorted(item.name for item in path.parent.iterdir()) == [path.name]
    assert key.hex() not in _log_text(caplog)
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_keys_differ_between_databases(tmp_path: Path) -> None:
    first = ensure_subject_key(tmp_path / "one" / "database.sqlite")
    second = ensure_subject_key(tmp_path / "two" / "database.sqlite")

    assert first is not None and second is not None and first != second


def test_load_never_creates_the_key(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    db_path.parent.mkdir(parents=True)

    assert load_subject_key(db_path) is None
    assert list(db_path.parent.iterdir()) == []


def test_key_id_is_a_short_sha256_prefix() -> None:
    key = bytes(range(32))

    assert subject_key_id(key) == hashlib.sha256(key).hexdigest()[:12]
    assert re.fullmatch(r"[0-9a-f]{12}", subject_key_id(key))


def test_key_id_mismatch_is_logged_without_the_key(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _db(tmp_path)
    key = ensure_subject_key(db_path)
    assert key is not None
    caplog.set_level(logging.INFO, logger="mailbot")

    assert ensure_subject_key(db_path, expected_key_id="0123456789ab") == key

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "subject_hash_key_changed" in warnings[0]
    assert "0123456789ab" in warnings[0] and subject_key_id(key) in warnings[0]
    assert key.hex() not in _log_text(caplog)


def test_lost_key_is_recreated_and_the_change_is_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _db(tmp_path)
    old_key = ensure_subject_key(db_path)
    assert old_key is not None
    subject_key_path(db_path).unlink()
    caplog.set_level(logging.INFO, logger="mailbot")

    new_key = ensure_subject_key(db_path, expected_key_id=subject_key_id(old_key))

    assert new_key is not None and new_key != old_key
    assert load_subject_key(db_path) == new_key
    log_text = _log_text(caplog)
    assert "subject_hash_key_created" in log_text
    assert "subject_hash_key_changed" in log_text


@pytest.mark.parametrize("content", [b"", b"not a key\n", b"ab" * 31 + b"\n", ("é" * 64).encode()])
def test_invalid_key_file_is_moved_aside_and_replaced(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, content: bytes
) -> None:
    db_path = _db(tmp_path)
    path = subject_key_path(db_path)
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    caplog.set_level(logging.INFO, logger="mailbot")

    assert load_subject_key(db_path) is None  # read-only callers never touch it
    assert path.read_bytes() == content
    assert "subject_hash_key_invalid" in _log_text(caplog)

    key = ensure_subject_key(db_path)

    assert key is not None and load_subject_key(db_path) == key
    moved = [item for item in path.parent.iterdir() if ".invalid-" in item.name]
    assert len(moved) == 1 and moved[0].read_bytes() == content
    assert "subject_hash_key_created" in _log_text(caplog)


def test_unreadable_key_degrades_and_is_never_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _db(tmp_path)
    key = ensure_subject_key(db_path)
    assert key is not None
    path = subject_key_path(db_path)
    content = path.read_bytes()

    def _deny(self: Path) -> bytes:
        raise PermissionError(13, "Permission denied", str(self))

    monkeypatch.setattr(Path, "read_bytes", _deny)
    caplog.set_level(logging.INFO, logger="mailbot")

    assert ensure_subject_key(db_path) is None
    assert load_subject_key(db_path) is None
    monkeypatch.undo()
    assert path.read_bytes() == content
    assert "subject_hash_key_unavailable" in _log_text(caplog)


def test_unwritable_folder_degrades_without_leftovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _db(tmp_path)
    db_path.parent.mkdir(parents=True)

    def _fail_replace(src: object, dst: object) -> None:
        raise PermissionError(13, "Permission denied", str(dst))

    monkeypatch.setattr(privacy_key.os, "replace", _fail_replace)
    caplog.set_level(logging.INFO, logger="mailbot")

    assert ensure_subject_key(db_path) is None
    monkeypatch.undo()
    assert list(db_path.parent.iterdir()) == []
    assert "subject_hash_key_unavailable" in _log_text(caplog)
    assert os.path.exists(subject_key_path(db_path)) is False
