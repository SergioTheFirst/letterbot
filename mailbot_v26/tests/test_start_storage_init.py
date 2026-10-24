"""start.main storage startup: schema refresh, schema guard, self-check target."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.tests.test_polling_loop import _build_imap, _install_common_patches

_ACCOUNT = AccountConfig(
    account_id="acc-1",
    login="account@example.com",
    password="pw",
    host="imap.example.com",
    port=993,
    use_ssl=True,
    telegram_chat_id="chat",
)


def _install(monkeypatch, tmp_path: Path) -> Path:
    """start.main with stubbed IMAP/Telegram on tmp_path/mailbot.sqlite."""
    monkeypatch.setattr(
        start_module, "ResilientIMAP", _build_imap({_ACCOUNT.login: [[]]})
    )
    _install_common_patches(monkeypatch, [_ACCOUNT], tmp_path)
    monkeypatch.setattr(start_module, "_check_build_integrity", lambda: None)
    return tmp_path / "mailbot.sqlite"


def _email_columns(db_path: Path) -> set[str]:
    with closing(sqlite3.connect(db_path)) as conn:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(emails);")}


def test_main_on_new_database_keeps_knowledge_columns_after_storage_init(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = _install(monkeypatch, tmp_path)
    monkeypatch.setattr(start_module, "Storage", Storage)
    assert not db_path.exists()

    start_module.main(max_cycles=1)

    # Storage recreated the table KnowledgeDB had created without uid; the
    # first run must still be able to save priority and summaries.
    assert {"uid", "priority", "action_line", "body_summary"} <= _email_columns(
        db_path
    )


def test_main_exits_2_on_populated_database_without_uid_and_keeps_rows(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    from mailbot_v26.storage.knowledge_db import KnowledgeDB
    from mailbot_v26.system.instance_lock import acquire_instance_lock

    db_path = _install(monkeypatch, tmp_path)
    monkeypatch.setattr(start_module, "Storage", Storage)
    kdb = KnowledgeDB(db_path)
    for index in range(2):
        kdb.save_email(
            account_email="account@example.com",
            from_email="sender@example.com",
            subject=f"Archived canary-subject {index}",
            received_at="2025-01-01T09:00:00+00:00",
            priority="🔵",
            action_line="Read",
            body_summary="Summary",
            raw_body="Body",
            attachment_summaries=[],
        )

    with pytest.raises(SystemExit) as exit_info:
        start_module.main(max_cycles=1)

    assert exit_info.value.code == 2
    output = capsys.readouterr().out
    assert str(db_path) in output
    assert "uid" in output
    assert "canary-subject" not in output
    with closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 2
    released = acquire_instance_lock(db_path)
    assert released is not None
    released.release()


def test_main_runs_self_check_on_the_configured_database(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = _install(monkeypatch, tmp_path)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(
        start_module,
        "run_self_check",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    start_module.main(max_cycles=1)

    assert len(calls) == 1
    args, kwargs = calls[0]
    target = kwargs.get("db_path", args[0] if args else None)
    assert target is not None and Path(target) == db_path
