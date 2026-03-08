from __future__ import annotations

from pathlib import Path

from mailbot_v26.tools.config_bootstrap import migrate_two_file_config


def test_migrate_creates_two_file_mode_and_backups(tmp_path: Path) -> None:
    (tmp_path / "config.ini").write_text(
        "[general]\ncheck_interval=120\n", encoding="utf-8"
    )
    (tmp_path / "keys.ini").write_text(
        "[telegram]\nbot_token=t\n[cloudflare]\naccount_id=a\napi_token=k\n",
        encoding="utf-8",
    )

    result = migrate_two_file_config(tmp_path)

    assert (tmp_path / "settings.ini").exists()
    assert (tmp_path / "accounts.ini").exists()
    assert any(path.name == "config.ini.bak" for path in result["backups"])
    assert any(path.name == "keys.ini.bak" for path in result["backups"])
