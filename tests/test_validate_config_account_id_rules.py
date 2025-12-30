from pathlib import Path

from mailbot_v26.tools.config_bootstrap import validate_config


def test_validate_config_account_id_rules(tmp_path: Path) -> None:
    (tmp_path / "config.ini").write_text(
        "[general]\ncheck_interval = 120\n\n[storage]\ndb_path = data/mailbot.sqlite\n",
        encoding="utf-8",
    )
    (tmp_path / "keys.ini").write_text(
        "[telegram]\nbot_token = CHANGE_ME\n\n[cloudflare]\naccount_id = CHANGE_ME\napi_token = CHANGE_ME\n",
        encoding="utf-8",
    )
    (tmp_path / "accounts.ini").write_text(
        "[Bad Account]\n"
        "login = user@example.com\n"
        "password = CHANGE_ME\n"
        "host = imap.example.com\n"
        "port = 993\n"
        "use_ssl = true\n"
        "telegram_chat_id = 12345\n",
        encoding="utf-8",
    )

    ok, errors = validate_config(tmp_path)

    assert not ok
    assert any("Invalid account_id" in error for error in errors)
