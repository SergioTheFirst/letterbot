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
import time
from pathlib import Path

_IS_WINDOWS = os.name == "nt"
if _IS_WINDOWS:
    import msvcrt
else:
    import fcntl

logger = logging.getLogger("mailbot")

# POSIX: a symlink planted at the lock path is refused, never followed (with
# O_CREAT the lock would create and land on whatever file it points to).
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)

# A probe (is_instance_running) or a worker that is just exiting holds the
# lock only for a moment: try this often before reporting "held".
_LOCK_ATTEMPTS = 3
_LOCK_RETRY_DELAY_SECONDS = 0.25

# errno values that mean "another process holds the lock". msvcrt reports a
# locked byte as EACCES (or EDEADLOCK); flock reports EWOULDBLOCK/EAGAIN, and
# an EACCES or EPERM there is a permission problem, never a held lock.
_WINDOWS_HELD_ERRNOS = frozenset(
    code
    for code in (errno.EACCES, getattr(errno, "EDEADLOCK", None))
    if code is not None
)
_POSIX_HELD_ERRNOS = frozenset((errno.EAGAIN, errno.EWOULDBLOCK))


class InstanceLockError(RuntimeError):
    """The lock could not be checked (fail closed: treat as unsafe to proceed)."""


def instance_lock_path(db_path: Path | str) -> Path:
    db = Path(db_path)
    return db.with_name(f"{db.name}.lock")


def _open_error(path: Path, exc: OSError) -> InstanceLockError:
    if isinstance(exc, PermissionError):
        return InstanceLockError(
            f"no permission to open the lock file {path} ({exc.strerror}); check "
            "the folder permissions and that no other program keeps the file open"
        )
    if _O_NOFOLLOW and exc.errno in (errno.ELOOP, errno.EMLINK):
        # ELOOP (Linux, macOS) / EMLINK (FreeBSD): O_NOFOLLOW met a symlink.
        return InstanceLockError(
            f"the lock file {path} is a symbolic link; refusing to follow it"
        )
    return InstanceLockError(f"cannot open {path}: {exc}")


def _try_lock(fd: int, path: Path) -> bool:
    """True when this descriptor now holds the lock, False when it is held."""
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        if _IS_WINDOWS:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        held_errnos = _WINDOWS_HELD_ERRNOS if _IS_WINDOWS else _POSIX_HELD_ERRNOS
        if exc.errno in held_errnos:
            return False
        if exc.errno in (errno.EACCES, errno.EPERM):
            raise InstanceLockError(
                f"no permission to lock {path} ({exc.strerror})"
            ) from exc
        raise InstanceLockError(f"cannot lock {path}: {exc}") from exc
    return True


def _try_lock_with_retry(fd: int, path: Path) -> bool:
    """_try_lock, retried briefly before the lock counts as held."""
    for attempt in range(1, _LOCK_ATTEMPTS + 1):
        if _try_lock(fd, path):
            return True
        if attempt < _LOCK_ATTEMPTS:
            logger.info(
                "single_instance_lock_busy lock=%s attempt=%d/%d: retrying",
                path,
                attempt,
                _LOCK_ATTEMPTS,
            )
            time.sleep(_LOCK_RETRY_DELAY_SECONDS)
    return False


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
        fd = os.open(path, os.O_RDWR | os.O_CREAT | _O_NOFOLLOW, 0o644)
    except OSError as exc:
        raise _open_error(path, exc) from exc
    try:
        acquired = _try_lock_with_retry(fd, path)
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
    # lexists: a symlink (even a dangling one) is opened and refused below.
    if not os.path.lexists(path):
        return False  # a running worker creates the file and never deletes it
    try:
        fd = os.open(path, os.O_RDWR | _O_NOFOLLOW)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise _open_error(path, exc) from exc
    try:
        if not _try_lock_with_retry(fd, path):
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
