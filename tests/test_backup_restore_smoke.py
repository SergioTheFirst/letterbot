from __future__ import annotations

import zipfile
from pathlib import Path

from mailbot_v26.tools import backup, restore


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_backup_restore_smoke(tmp_path: Path) -> None:
    _write(tmp_path / "data" / "mailbot.sqlite", "sqlite")
    _write(tmp_path / "state.json", '{"state": 1}')
    _write(tmp_path / "mailbot_v26" / "runtime_flags.json", '{"enable_gigachat": true}')
    _write(tmp_path / "mailbot_v26" / "data" / "runtime_health.json", '{"ok": true}')

    _write(
        tmp_path / "mailbot_v26" / "config" / "config.ini",
        "[general]\nadmin_chat_id=123\n",
    )
    _write(
        tmp_path / "mailbot_v26" / "config" / "accounts.ini",
        "[acc]\nlogin=user@example.com\npassword=supersecret\n",
    )
    _write(
        tmp_path / "mailbot_v26" / "config" / "keys.ini",
        "[telegram]\nbot_token=secret-token\n",
    )

    result = backup.create_backup(base_dir=tmp_path, retention=2)
    assert result.archive_path.exists()

    with zipfile.ZipFile(result.archive_path) as archive:
        accounts_content = archive.read("mailbot_v26/config/accounts.ini").decode(
            "utf-8"
        )
        keys_content = archive.read("mailbot_v26/config/keys.ini").decode("utf-8")

    assert "supersecret" not in accounts_content
    assert "secret-token" not in keys_content
    assert "***" in accounts_content
    assert "***" in keys_content

    _write(tmp_path / "state.json", '{"state": 2}')
    restore.restore_from_backup(result.archive_path, base_dir=tmp_path)

    restored_state = (tmp_path / "state.json").read_text(encoding="utf-8")
    assert '"state": 1' in restored_state
