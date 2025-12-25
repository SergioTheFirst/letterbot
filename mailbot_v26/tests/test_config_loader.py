from pathlib import Path

import pytest

from mailbot_v26.config_loader import (
    BotConfig,
    ConfigError,
    InvalidAccountIdError,
    load_accounts_config,
    load_config,
    load_general_config,
    load_keys_config,
    load_storage_config,
)


def write_file(tmpdir: Path, name: str, content: str) -> None:
    path = tmpdir / name
    path.write_text(content, encoding="utf-8")


def build_sample_config(tmpdir: Path) -> None:
    write_file(
        tmpdir,
        "config.ini",
        """[general]
check_interval = 400
max_email_mb = 25
max_attachment_mb = 20
max_zip_uncompressed_mb = 90
max_extracted_chars = 40000
max_extracted_total_chars = 110000
admin_chat_id = 111

[storage]
db_path = data/custom.sqlite
""",
    )
    write_file(
        tmpdir,
        "accounts.ini",
        """[primary]
login = sample@example.com
password = secret
host = imap.example.com
port = 993
use_ssl = true
telegram_chat_id = 222
""",
    )
    write_file(
        tmpdir,
        "keys.ini",
        """[telegram]
bot_token = token

[cloudflare]
account_id = acc
api_token = key
""",
    )


def test_load_full_config(tmp_path: Path) -> None:
    build_sample_config(tmp_path)
    cfg = load_config(tmp_path)
    assert isinstance(cfg, BotConfig)
    assert cfg.general.check_interval == 400
    assert cfg.general.max_email_mb == 25
    assert cfg.accounts[0].account_id == "primary"
    assert cfg.accounts[0].login == "sample@example.com"
    assert cfg.keys.telegram_bot_token == "token"
    storage_cfg = load_storage_config(tmp_path)
    assert cfg.storage.db_path == storage_cfg.db_path


def test_missing_files_raise() -> None:
    with pytest.raises(ConfigError):
        load_general_config(Path("/nonexistent"))


def test_accounts_missing_section(tmp_path: Path) -> None:
    write_file(tmp_path, "accounts.ini", "")
    with pytest.raises(ConfigError):
        load_accounts_config(tmp_path)


def test_accounts_invalid_section_name(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "accounts.ini",
        """[Bad-Name]
login = sample@example.com
password = secret
host = imap.example.com
port = 993
use_ssl = true
telegram_chat_id = 222
""",
    )
    with pytest.raises(InvalidAccountIdError):
        load_accounts_config(tmp_path)


def test_general_default_interval(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "config.ini",
        """[general]
max_attachment_mb = 10
admin_chat_id = 1
""",
    )
    general = load_general_config(tmp_path)
    assert general.check_interval == 180
    assert general.max_email_mb == 15


def test_general_interval_explicit_value(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "config.ini",
        """[general]
check_interval = 180
max_attachment_mb = 10
admin_chat_id = 1
""",
    )
    general = load_general_config(tmp_path)
    assert general.check_interval == 180
    assert general.max_zip_uncompressed_mb == 80
