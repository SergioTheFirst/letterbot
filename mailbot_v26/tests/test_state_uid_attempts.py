"""WP2a state: UID attempt counts and the dead-letter alert time live in
top-level state.json keys (F8: never AccountState fields), and save() fsyncs
the temp file before it replaces the live state."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26 import state_manager
from mailbot_v26.state_manager import AccountState, StateManager

LOGIN = "user@example.com"


def test_uid_attempts_persist_as_top_level_key(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    state = StateManager(path)
    state.update_last_uid(LOGIN, 4)
    state.set_uid_attempts("User@Example.com", {5: 2, 7: 1})
    state.save(force=True)

    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["uid_attempts"] == {LOGIN: {"5": 2, "7": 1}}
    # F8: AccountState(**data) on an older build must keep loading this file
    assert set(raw["accounts"][LOGIN]) == set(AccountState().__dict__)

    reloaded = StateManager(path)
    assert reloaded.get_uid_attempts("USER@example.com") == {5: 2, 7: 1}
    assert reloaded.get_last_uid(LOGIN) == 4


def test_empty_uid_attempts_drop_the_login_entry(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    state = StateManager(path)
    state.set_uid_attempts(LOGIN, {5: 1})
    state.set_uid_attempts(LOGIN, {})
    state.save(force=True)

    assert json.loads(path.read_text(encoding="utf-8"))["uid_attempts"] == {}


def test_legacy_state_without_new_keys_loads(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps({"accounts": {LOGIN: {"last_uid": 3}}, "llm": {}, "meta": {}}),
        encoding="utf-8",
    )

    state = StateManager(path)

    assert state.get_last_uid(LOGIN) == 3
    assert state.get_uid_attempts(LOGIN) == {}
    assert state.get_dead_letter_alert_at(LOGIN) is None


def test_malformed_uid_attempt_entries_are_ignored(tmp_path: Path, caplog) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "accounts": {},
                "uid_attempts": {LOGIN: {"5": 2, "x": 1, "6": None}, "": {"1": 1}},
                "dead_letter_alert_at": {LOGIN: 7},
            }
        ),
        encoding="utf-8",
    )

    state = StateManager(path)

    assert state.get_uid_attempts(LOGIN) == {5: 2}
    assert state.get_dead_letter_alert_at(LOGIN) is None
    assert "state_uid_attempt_entry_ignored" in caplog.text


def test_dead_letter_alert_time_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    sent_at = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    state = StateManager(path)
    state.set_dead_letter_alert_at(LOGIN, sent_at)
    state.save(force=True)

    assert StateManager(path).get_dead_letter_alert_at("User@Example.com") == sent_at


def test_save_fsyncs_temp_file_before_replacing_state(
    monkeypatch, tmp_path: Path
) -> None:
    path = tmp_path / "state.json"
    observed: list[bool] = []
    real_fsync = state_manager.os.fsync

    def _recording_fsync(fd: int) -> None:
        # the live state file is replaced only after the data hit the disk
        observed.append(path.exists())
        real_fsync(fd)

    monkeypatch.setattr(state_manager.os, "fsync", _recording_fsync)
    state = StateManager(path)
    state.update_last_uid(LOGIN, 9)
    state.save(force=True)

    assert observed == [False]
    assert StateManager(path).get_last_uid(LOGIN) == 9
