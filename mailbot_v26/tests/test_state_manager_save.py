"""P2SEC: StateManager.save() writes through a unique temp file in the state
directory, one writer at a time per process, and removes its temp file when
the save fails (the error still propagates and the change stays pending)."""

from __future__ import annotations

import contextlib
import errno
import json
import logging
import os
import stat
import threading
from pathlib import Path

import pytest

from mailbot_v26 import state_manager
from mailbot_v26.state_manager import StateManager

LOGIN = "user@example.com"


def _temp_files(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if p.name.endswith(".tmp"))


def _saved_uid(path: Path) -> int:
    return json.loads(path.read_text(encoding="utf-8"))["accounts"][LOGIN]["last_uid"]


def test_save_never_touches_a_temp_file_it_does_not_own(tmp_path: Path) -> None:
    # The old fixed name: another writer (or an older build) may be mid-write.
    foreign = tmp_path / "state.tmp"
    foreign.write_text("foreign", encoding="utf-8")
    state = StateManager(tmp_path / "state.json")
    state.update_last_uid(LOGIN, 5)

    state.save(force=True)

    assert foreign.read_text(encoding="utf-8") == "foreign"
    assert _saved_uid(tmp_path / "state.json") == 5
    assert _temp_files(tmp_path) == ["state.tmp"]


def test_save_writes_a_unique_temp_file_beside_the_state_file(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "state.json"
    real_replace = os.replace
    renames: list[tuple[Path, Path]] = []

    def _recording_replace(src, dst) -> None:
        renames.append((Path(src), Path(dst)))
        real_replace(src, dst)

    monkeypatch.setattr(state_manager.os, "replace", _recording_replace)
    state = StateManager(path)
    state.update_last_uid(LOGIN, 1)
    state.save(force=True)
    state.update_last_uid(LOGIN, 2)
    state.save(force=True)

    assert len(renames) == 2
    (first, first_dst), (second, second_dst) = renames
    assert first != second
    # same directory: the rename stays on one filesystem and is atomic
    assert first.parent == second.parent == path.parent
    assert first_dst == second_dst == path
    assert _saved_uid(path) == 2
    assert _temp_files(tmp_path) == []


def test_failed_save_removes_its_temp_file_and_keeps_the_last_state(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    path = tmp_path / "state.json"
    state = StateManager(path)
    state.update_last_uid(LOGIN, 5)
    state.save(force=True)
    state.update_last_uid(LOGIN, 6)
    real_fsync = os.fsync

    def _disk_full(_fd: int) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(state_manager.os, "fsync", _disk_full)
    with caplog.at_level(logging.WARNING, logger=state_manager.__name__):
        with pytest.raises(OSError):
            state.save(force=True)

    assert _temp_files(tmp_path) == []
    assert _saved_uid(path) == 5
    assert "state_save_failed" in caplog.text

    # the change stays pending: the next save writes it
    monkeypatch.setattr(state_manager.os, "fsync", real_fsync)
    state.save()
    assert _saved_uid(path) == 6


def test_failed_rename_removes_the_temp_file(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.mkdir()  # nothing can be renamed over a non-empty directory
    (path / "keep").write_text("x", encoding="utf-8")
    state = StateManager(path)
    state.update_last_uid(LOGIN, 1)

    with pytest.raises(OSError):
        state.save(force=True)

    assert _temp_files(tmp_path) == []


def test_saves_of_two_managers_on_one_file_are_serialized(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "state.json"
    first, second = StateManager(path), StateManager(path)
    first.update_last_uid(LOGIN, 1)
    second.update_last_uid(LOGIN, 2)
    real_fsync = os.fsync
    # Without serialization both writers meet here, mid-write.
    meeting = threading.Barrier(2)
    inside = {"now": 0, "max": 0}
    counter_lock = threading.Lock()

    def _meeting_fsync(fd: int) -> None:
        with counter_lock:
            inside["now"] += 1
            inside["max"] = max(inside["max"], inside["now"])
        with contextlib.suppress(threading.BrokenBarrierError):
            meeting.wait(timeout=0.5)
        real_fsync(fd)
        with counter_lock:
            inside["now"] -= 1

    monkeypatch.setattr(state_manager.os, "fsync", _meeting_fsync)
    errors: list[BaseException] = []

    def _save(manager: StateManager) -> None:
        try:
            manager.save(force=True)
        except BaseException as exc:  # reported by the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=_save, args=(m,)) for m in (first, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert inside["max"] == 1
    assert _saved_uid(path) in (1, 2)
    assert _temp_files(tmp_path) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
def test_saved_state_file_is_private(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    state = StateManager(path)
    state.update_last_uid(LOGIN, 1)

    state.save(force=True)

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
