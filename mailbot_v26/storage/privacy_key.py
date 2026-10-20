"""Local key for subject hashes in event payloads (``<db>.subject.key``).

Reminder-chain detection compares normalized subjects across senders. Event
payloads store HMAC-SHA256(key, subject) instead of the subject, so the key
must be stable and must stay on this computer:

- it lives next to the database, named like the instance lock (``<db>.lock``);
- only the worker (or a maintenance tool holding the instance lock) creates
  it, with an atomic write (temp file + ``os.replace``); every other process
  only reads it;
- it is never logged, exported or sent anywhere; backups carry it together
  with the database, the support export leaves it out.

``key_id`` (first 12 hex chars of its SHA-256) names a key without revealing
it, so a lost or replaced key is detected and logged: hashes stored under the
old key no longer match, and cross-sender reminder chains that span the
change are missed until the 14-day window has passed.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("mailbot")

SUBJECT_KEY_BYTES = 32
KEY_ID_HEX_CHARS = 12
_KEY_FILE_RE = re.compile(rb"[0-9a-fA-F]{%d}" % (SUBJECT_KEY_BYTES * 2))


def subject_key_path(db_path: Path | str) -> Path:
    db = Path(db_path)
    return db.with_name(f"{db.name}.subject.key")


def subject_key_id(key: bytes) -> str:
    return hashlib.sha256(key).hexdigest()[:KEY_ID_HEX_CHARS]


def _parse_key(raw: bytes) -> bytes | None:
    text = raw.strip()
    if not _KEY_FILE_RE.fullmatch(text):
        return None
    return bytes.fromhex(text.decode("ascii"))


def _read_key_file(path: Path) -> tuple[bytes | None, bool]:
    """(key, file_exists); raises OSError when the file cannot be read."""
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None, False
    return _parse_key(raw), True


def load_subject_key(db_path: Path | str) -> bytes | None:
    """Key for this database, or None (read-only: never creates or changes it)."""
    path = subject_key_path(db_path)
    try:
        key, exists = _read_key_file(path)
    except OSError as exc:
        logger.warning(
            "subject_hash_key_unavailable path=%s reason=unreadable error=%s", path, exc
        )
        return None
    if exists and key is None:
        logger.warning("subject_hash_key_invalid path=%s", path)
    return key


def ensure_subject_key(
    db_path: Path | str, *, expected_key_id: str | None = None
) -> bytes | None:
    """Load or create the key; worker / maintenance only (holds the instance lock).

    A missing key is created. A file with invalid content is moved aside to
    ``<key>.invalid-<UTC time>`` (never deleted) and replaced. A key that
    cannot be read or written gives None: subject hashing degrades and an
    existing file is never overwritten. ``expected_key_id`` is the id recorded
    when stored hashes were written; a different key is logged.
    """
    path = subject_key_path(db_path)
    try:
        key, exists = _read_key_file(path)
    except OSError as exc:
        logger.error(
            "subject_hash_key_unavailable path=%s reason=unreadable error=%s", path, exc
        )
        return None
    if key is None:
        if exists and not _move_invalid_key_aside(path):
            return None
        key = _create_key(path)
        if key is None:
            return None
    actual_key_id = subject_key_id(key)
    if expected_key_id and expected_key_id != actual_key_id:
        logger.warning(
            "subject_hash_key_changed path=%s key_id=%s expected_key_id=%s",
            path,
            actual_key_id,
            expected_key_id,
        )
    return key


def _move_invalid_key_aside(path: Path) -> bool:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    target = path.with_name(f"{path.name}.invalid-{stamp}")
    try:
        os.replace(path, target)
    except OSError as exc:
        logger.error(
            "subject_hash_key_unavailable path=%s reason=invalid_not_moved error=%s",
            path,
            exc,
        )
        return False
    logger.warning("subject_hash_key_invalid path=%s moved_to=%s", path, target)
    return True


def _create_key(path: Path) -> bytes | None:
    key = secrets.token_bytes(SUBJECT_KEY_BYTES)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(
            tmp_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0),
            0o600,
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(key.hex().encode("ascii") + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except OSError as exc:
        logger.error(
            "subject_hash_key_unavailable path=%s reason=create_failed error=%s",
            path,
            exc,
        )
        _remove_temp_file(tmp_path)
        return None
    logger.info(
        "subject_hash_key_created path=%s key_id=%s", path, subject_key_id(key)
    )
    return key


def _remove_temp_file(tmp_path: Path) -> None:
    try:
        tmp_path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        logger.warning(
            "subject_hash_key_temp_cleanup_failed path=%s error=%s", tmp_path, exc
        )


__all__ = [
    "KEY_ID_HEX_CHARS",
    "SUBJECT_KEY_BYTES",
    "ensure_subject_key",
    "load_subject_key",
    "subject_key_id",
    "subject_key_path",
]
