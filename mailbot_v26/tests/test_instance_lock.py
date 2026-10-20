"""P2SEC: single-instance lock hardening.

A probe (is_instance_running) or a worker that is just exiting holds the lock
only for a moment, so both callers retry briefly before they report "held".
On POSIX the lock file is opened without following a symlink, and a
permission problem is reported as such instead of "already running" (msvcrt
reports a byte locked by another process as EACCES, flock never does).
"""

from __future__ import annotations

import errno
import logging
import os
from pathlib import Path

import pytest

from mailbot_v26.system import instance_lock
from mailbot_v26.system.instance_lock import (
    InstanceLockError,
    acquire_instance_lock,
    instance_lock_path,
    is_instance_running,
)


@pytest.fixture()
def sleeps(monkeypatch) -> list[float]:
    recorded: list[float] = []
    monkeypatch.setattr("time.sleep", recorded.append)
    return recorded


class _FakeLockModule:
    """fcntl / msvcrt stand-in whose lock call fails with `error`."""

    LOCK_EX, LOCK_NB, LOCK_UN = 2, 4, 8  # fcntl
    LK_UNLCK, LK_NBLCK = 0, 2  # msvcrt

    def __init__(self, error: OSError) -> None:
        self.error = error
        self.lock_calls = 0

    def flock(self, _fd: int, operation: int) -> None:
        if operation & self.LOCK_UN:
            return
        self.lock_calls += 1
        raise self.error

    def locking(self, _fd: int, mode: int, _nbytes: int) -> None:
        if mode == self.LK_UNLCK:
            return
        self.lock_calls += 1
        raise self.error


def _fake_platform(monkeypatch, *, windows: bool, error: OSError) -> _FakeLockModule:
    fake = _FakeLockModule(error)
    monkeypatch.setattr(instance_lock, "_IS_WINDOWS", windows)
    monkeypatch.setattr(
        instance_lock, "msvcrt" if windows else "fcntl", fake, raising=False
    )
    return fake


# ---- brief retry before "held" ----


def test_acquire_retries_a_lock_released_meanwhile(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    holder = acquire_instance_lock(db_path)
    assert holder is not None
    waits: list[float] = []

    def _sleep(seconds: float) -> None:
        waits.append(seconds)
        holder.release()  # the probe or the exiting worker lets go

    monkeypatch.setattr("time.sleep", _sleep)
    try:
        with caplog.at_level(logging.INFO, logger="mailbot"):
            lock = acquire_instance_lock(db_path)
        assert lock is not None
        lock.release()
    finally:
        holder.release()

    assert len(waits) == 1
    assert 0 < waits[0] <= 1
    assert "single_instance_lock_busy" in caplog.text


def test_acquire_reports_held_after_two_retries(
    tmp_path: Path, sleeps: list[float]
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    holder = acquire_instance_lock(db_path)
    assert holder is not None
    try:
        assert acquire_instance_lock(db_path) is None
    finally:
        holder.release()

    assert len(sleeps) == 2


def test_probe_retries_before_reporting_a_running_instance(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    holder = acquire_instance_lock(db_path)
    assert holder is not None
    monkeypatch.setattr("time.sleep", lambda _seconds: holder.release())
    try:
        assert is_instance_running(db_path) is False
    finally:
        holder.release()


def test_probe_reports_running_while_the_lock_stays_held(
    tmp_path: Path, sleeps: list[float]
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    holder = acquire_instance_lock(db_path)
    assert holder is not None
    try:
        assert is_instance_running(db_path) is True
    finally:
        holder.release()

    assert len(sleeps) == 2


# ---- held vs permission ----


def test_windows_lock_violation_counts_as_held(
    tmp_path: Path, monkeypatch, sleeps: list[float]
) -> None:
    # msvcrt.locking reports a byte locked by another process as EACCES.
    fake = _fake_platform(
        monkeypatch, windows=True, error=PermissionError(errno.EACCES, "denied")
    )

    assert acquire_instance_lock(tmp_path / "mailbot.sqlite") is None
    assert fake.lock_calls == 3


def test_posix_would_block_counts_as_held(
    tmp_path: Path, monkeypatch, sleeps: list[float]
) -> None:
    fake = _fake_platform(
        monkeypatch,
        windows=False,
        error=BlockingIOError(errno.EWOULDBLOCK, "would block"),
    )

    assert acquire_instance_lock(tmp_path / "mailbot.sqlite") is None
    assert fake.lock_calls == 3


@pytest.mark.parametrize("code", [errno.EACCES, errno.EPERM], ids=["EACCES", "EPERM"])
def test_posix_permission_error_while_locking_is_not_reported_as_held(
    tmp_path: Path, monkeypatch, sleeps: list[float], code: int
) -> None:
    fake = _fake_platform(
        monkeypatch, windows=False, error=PermissionError(code, "denied")
    )

    with pytest.raises(InstanceLockError, match="no permission"):
        acquire_instance_lock(tmp_path / "mailbot.sqlite")
    assert fake.lock_calls == 1  # not retried: it will not go away
    assert sleeps == []


def test_permission_denied_opening_the_lock_file_is_reported_as_such(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    lock_path = instance_lock_path(db_path)
    lock_path.write_bytes(b"")

    def _denied(*_args, **_kwargs):
        raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(instance_lock.os, "open", _denied)
    for check in (acquire_instance_lock, is_instance_running):
        with pytest.raises(InstanceLockError, match="no permission") as info:
            check(db_path)
        assert str(lock_path) in str(info.value)


# ---- symlinks (POSIX) ----


def test_lock_file_is_opened_without_following_symlinks(
    tmp_path: Path, monkeypatch
) -> None:
    sentinel = 1 << 30  # stands in for O_NOFOLLOW on any platform
    real_open = os.open
    seen: list[int] = []

    def _open(path, flags, *args):
        seen.append(flags)
        return real_open(path, flags & ~sentinel, *args)

    monkeypatch.setattr(instance_lock, "_O_NOFOLLOW", sentinel, raising=False)
    monkeypatch.setattr(instance_lock.os, "open", _open)
    db_path = tmp_path / "mailbot.sqlite"

    lock = acquire_instance_lock(db_path)
    assert lock is not None
    lock.release()
    assert is_instance_running(db_path) is False

    assert len(seen) == 2
    assert all(flags & sentinel for flags in seen)


@pytest.mark.skipif(os.name == "nt", reason="O_NOFOLLOW is POSIX-only")
def test_symlinked_lock_path_is_refused_without_touching_the_target(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    target = tmp_path / "elsewhere" / "victim.txt"
    target.parent.mkdir()
    # Dangling: following it, O_CREAT would create the target file.
    instance_lock_path(db_path).symlink_to(target)

    with pytest.raises(InstanceLockError, match="symbolic link"):
        acquire_instance_lock(db_path)
    assert not target.exists()

    with pytest.raises(InstanceLockError, match="symbolic link"):
        is_instance_running(db_path)
    assert not target.exists()
