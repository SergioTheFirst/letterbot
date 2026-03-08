from datetime import datetime
import json
from pathlib import Path

from mailbot_v26.state_manager import StateManager


def test_state_persistence(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    manager = StateManager(state_path)
    manager.update_last_uid("user@example.com", 10)
    manager.update_check_time("user@example.com", datetime(2024, 1, 1))
    manager.add_tokens(50)
    manager.save(force=True)

    manager2 = StateManager(state_path)
    assert manager2.get_last_uid("user@example.com") == 10
    ts = manager2.get_last_check_time("user@example.com")
    assert ts is not None and ts.year == 2024
    assert manager2._state.llm.tokens_used_today >= 50


def test_llm_unavailable_flag(tmp_path: Path) -> None:
    manager = StateManager(tmp_path / "state.json")
    manager.set_llm_unavailable(True, "maintenance")
    manager.save(force=True)
    reloaded = StateManager(tmp_path / "state.json")
    assert reloaded._state.llm.unavailable is True
    assert reloaded._state.llm.last_error == "maintenance"


def test_state_normalizes_login_keys_and_merges_legacy_variants(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "accounts": {
                    "User@Example.com": {
                        "last_uid": 5,
                        "last_check_time": "2024-01-01T10:00:00",
                        "imap_status": "ok",
                        "last_error": "",
                    },
                    "user@example.com": {
                        "last_uid": 7,
                        "last_check_time": "2024-01-01T12:00:00",
                        "imap_status": "failed",
                        "last_error": "timeout",
                    },
                },
                "llm": {},
                "meta": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    manager = StateManager(state_path)

    assert manager.get_last_uid("USER@example.com") == 7
    assert manager.get_last_check_time("user@example.com") == datetime.fromisoformat(
        "2024-01-01T12:00:00"
    )

    manager.update_last_uid("USER@EXAMPLE.COM", 9)
    manager.save(force=True)

    reloaded = json.loads(state_path.read_text(encoding="utf-8"))
    assert set(reloaded.get("accounts", {}).keys()) == {"user@example.com"}
    assert reloaded["accounts"]["user@example.com"]["last_uid"] == 9


def test_uidvalidity_roundtrip_and_cursor_reset(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    manager = StateManager(state_path)
    manager.update_last_uid("user@example.com", 77)
    manager.update_uidvalidity("user@example.com", 1234, mailbox="INBOX")
    manager.save(force=True)

    reloaded = StateManager(state_path)
    assert reloaded.get_uidvalidity("USER@example.com") == 1234

    reloaded.reset_account_cursor("user@example.com")
    assert reloaded.get_last_uid("user@example.com") == 0
    assert reloaded.get_last_check_time("user@example.com") is None
