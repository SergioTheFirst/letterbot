"""start.main storage startup: schema refresh, schema guard, self-check target."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

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
