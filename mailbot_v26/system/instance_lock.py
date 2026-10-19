"""Single-instance guard for the Letterbot worker (stdlib only).

Two workers on one database would notify every email twice, so the worker
holds an OS-level lock on ``<db>.lock`` for its whole run. The OS drops the
lock when the process dies, so a crash never leaves a stale lock; the file
itself is never deleted (deleting it would let a second process lock a new
file while the first still holds the old one).
"""

from __future__ import annotations

import errno
import logging
import os
from pathlib import Path

_IS_WINDOWS = os.name == "nt"
if _IS_WINDOWS:
    import msvcrt
else:
    import fcntl

logger = logging.getLogger("mailbot")

# errno values that mean "another process holds the lock".
_HELD_ERRNOS = frozenset(
    code
    for code in (
        errno.EACCES,
        getattr(errno, "EDEADLOCK", None),
        errno.EAGAIN,
        errno.EWOULDBLOCK,
    )
    if code is not None
)


class InstanceLockError(RuntimeError):
    """The lock could not be checked (fail closed: treat as unsafe to proceed)."""


def instance_lock_path(db_path: Path | str) -> Path:
    db = Path(db_path)
    return db.with_name(f"{db.name}.lock")


def _try_lock(fd: int, path: Path) -> bool:
    """True when this descriptor now holds the lock, False when it is held."""
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        if _IS_WINDOWS:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in _HELD_ERRNOS:
            return False
        raise InstanceLockError(f"cannot lock {path}: {exc}") from exc
    return True


def _unlock(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if _IS_WINDOWS:
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


class InstanceLock:
    def __init__(self, path: Path, fd: int) -> None:
        self.path = path
        self._fd: int | None = fd

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            _unlock(fd)
        except OSError as exc:
            # Closing the descriptor below still drops the lock.
            logger.warning("single_instance_unlock_failed lock=%s error=%s", self.path, exc)
        finally:
            os.close(fd)


def acquire_instance_lock(db_path: Path | str) -> InstanceLock | None:
    """Lock for this database, or None when another instance holds it."""
    path = instance_lock_path(db_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError as exc:
        raise InstanceLockError(f"cannot open {path}: {exc}") from exc
    try:
        acquired = _try_lock(fd, path)
    except BaseException:
        os.close(fd)
        raise
    if not acquired:
        os.close(fd)
        return None
    return InstanceLock(path, fd)


def is_instance_running(db_path: Path | str) -> bool:
    """True while a worker holds the lock for this database."""
    path = instance_lock_path(db_path)
    if not path.exists():
        return False  # a running worker creates the file and never deletes it
    try:
        fd = os.open(path, os.O_RDWR)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise InstanceLockError(f"cannot open {path}: {exc}") from exc
    try:
        if not _try_lock(fd, path):
            return True
        try:
            _unlock(fd)
        except OSError as exc:
            # Closing the descriptor below drops the probe's lock anyway.
            logger.warning("single_instance_probe_unlock_failed lock=%s error=%s", path, exc)
        return False
    finally:
        os.close(fd)


__all__ = [
    "InstanceLock",
    "InstanceLockError",
    "acquire_instance_lock",
    "instance_lock_path",
    "is_instance_running",
]
