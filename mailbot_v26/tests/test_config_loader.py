from pathlib import Path

import pytest

from mailbot_v26.config_loader import (
    BotConfig,
            load_accounts_config,
    load_config,
    load_general_config,
    load_keys_config,
    parse_telegram_chat_id,
    load_storage_config,
    load_support_settings,
    validate_telegram_contract,
    load_telegram_ui_config,
    load_web_config,
    load_web_ui_password_from_ini,
)


def write_file(tmpdir: Path, name: str, content: str) -> None:
    path = tmpdir / name
    path.write_text(content, encoding="utf-8")


def build_sample_config(tmpdir: Path) -> None:
    write_file(
        tmpdir,
        "settings.ini",
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

[telegram]
bot_token = token
chat_id = -100200300

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
    assert cfg.keys.telegram_chat_id == "-100200300"
    storage_cfg = load_storage_config(tmp_path)
    assert cfg.storage.db_path == storage_cfg.db_path
    assert cfg.accounts[0].telegram_chat_id == "222"


def test_accounts_use_global_telegram_chat_id_when_account_override_missing(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "accounts.ini",
        """[primary]
login = sample@example.com
password = secret
host = imap.example.com

[telegram]
bot_token = 123:abc
chat_id = -100555666
""",
    )

    accounts = load_accounts_config(tmp_path)

    assert len(accounts) == 1
    assert accounts[0].telegram_chat_id == "-100555666"


def test_parse_telegram_chat_id_rejects_invalid_value() -> None:
    assert parse_telegram_chat_id("=272123") == ""
    assert parse_telegram_chat_id("-100272123") == "-100272123"


def test_validate_telegram_contract_fails_fast_on_missing_credentials(tmp_path: Path) -> None:
    build_sample_config(tmp_path)
    (tmp_path / "accounts.ini").write_text(
        """[primary]
login = sample@example.com
password = secret
host = imap.example.com

[telegram]
bot_token = CHANGE_ME
chat_id = =272123
""",
        encoding="utf-8",
    )
    cfg = load_config(tmp_path)

    errors = validate_telegram_contract(cfg, config_dir=tmp_path)

    assert any("bot_token" in item for item in errors)
    assert any("chat_id" in item for item in errors)




def test_validate_telegram_contract_allows_missing_token_without_telegram_targets(tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text("""[general]
check_interval=120
""", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        """[primary]
login = sample@example.com
password = secret
host = imap.example.com

[telegram]
bot_token = CHANGE_ME
chat_id =
""",
        encoding="utf-8",
    )
    cfg = load_config(tmp_path)

    errors = validate_telegram_contract(cfg, config_dir=tmp_path)

    assert not any("bot_token" in item for item in errors)


def test_validate_telegram_contract_rejects_account_chat_id_with_leading_equal(tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text("""[general]
check_interval=120
""", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        """[primary]
login = sample@example.com
password = secret
host = imap.example.com
telegram_chat_id = =272250747

[telegram]
bot_token = 123:abc
""",
        encoding="utf-8",
    )
    cfg = load_config(tmp_path)

    errors = validate_telegram_contract(cfg, config_dir=tmp_path)

    assert any("[primary].telegram_chat_id" in item for item in errors)
def test_missing_files_use_deterministic_defaults() -> None:
    general = load_general_config(Path("/nonexistent"))
    assert general.check_interval == 120


def test_accounts_missing_section_returns_empty_list(tmp_path: Path) -> None:
    write_file(tmp_path, "accounts.ini", "")
    assert load_accounts_config(tmp_path) == []


def test_accounts_invalid_section_name_is_ignored(tmp_path: Path) -> None:
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

[telegram]
bot_token = token

[cloudflare]
account_id = acc
api_token = key
""",
    )
    assert load_accounts_config(tmp_path) == []


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
    assert general.check_interval == 120
    assert general.max_email_mb == 15


def test_general_interval_explicit_value(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "config.ini",
        """[general]
check_interval = 120
max_attachment_mb = 10
admin_chat_id = 1
""",
    )
    general = load_general_config(tmp_path)
    assert general.check_interval == 120
    assert general.max_zip_uncompressed_mb == 80


def test_web_settings_loads_from_settings_ini(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "settings.ini",
        """[web]
host = 0.0.0.0
port = 9999
""",
    )
    write_file(
        tmp_path,
        "accounts.ini",
        """[acc]
login = u@example.com
password = p
host = imap.example.com
""",
    )

    web = load_web_config(tmp_path)

    assert web.host == "0.0.0.0"
    assert web.port == 9999


def test_web_settings_default_host_and_port(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "settings.ini",
        """[general]
check_interval = 120
""",
    )
    write_file(
        tmp_path,
        "accounts.ini",
        """[acc]
login = u@example.com
password = p
host = imap.example.com
""",
    )

    web = load_web_config(tmp_path)

    assert web.host == "127.0.0.1"
    assert web.port == 8787


def test_load_support_settings_two_file_mode_ignores_yaml(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "settings.ini",
        """[support]
enabled = true
text = Support text
url = https://example.com/donate
label = Поддержать
frequency_days = 45
""",
    )
    write_file(
        tmp_path,
        "accounts.ini",
        """[acc]
login = u@example.com
password = p
host = imap.example.com
""",
    )
    write_file(
        tmp_path,
        "config.yaml",
        "support:\n  enabled: false\n",
    )

    from mailbot_v26 import config_loader as loader

    original = loader._load_support_from_yaml

    def _boom(_raw):
        raise AssertionError("YAML fallback must not be used in 2-file mode")

    loader._load_support_from_yaml = _boom
    try:
        support = loader.load_support_settings(tmp_path)
    finally:
        loader._load_support_from_yaml = original

    assert support.enabled is True
    assert support.text == "Support text"
    assert support.url == "https://example.com/donate"
    assert support.label == "Поддержать"
    assert support.frequency_days == 45


def test_load_support_settings_defaults_without_section(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "settings.ini",
        """[general]
check_interval = 120
""",
    )
    write_file(
        tmp_path,
        "accounts.ini",
        """[acc]
login = u@example.com
password = p
host = imap.example.com
""",
    )

    support = load_support_settings(tmp_path)

    assert support.enabled is False
    assert support.text == "Если Letterbot помогает, проект можно поддержать"
    assert support.url == "CHANGE_ME"
    assert support.label == "Поддержать Letterbot"
    assert support.frequency_days == 30


def test_load_support_settings_invalid_frequency_falls_back_to_default(tmp_path: Path) -> None:
    write_file(
        tmp_path,
        "settings.ini",
        """[support]
enabled = true
text = t
url = https://example.com
label = l
frequency_days = bad
""",
    )
    write_file(
        tmp_path,
        "accounts.ini",
        """[acc]
login = u@example.com
password = p
host = imap.example.com
""",
    )

    support = load_support_settings(tmp_path)

    assert support.enabled is True
    assert support.frequency_days == 30


def test_load_telegram_ui_config_defaults_to_hidden(tmp_path: Path) -> None:
    write_file(tmp_path, "settings.ini", """[general]
check_interval=120
""")
    write_file(tmp_path, "accounts.ini", """[acc]
login=u
password=p
host=h
""")

    cfg = load_telegram_ui_config(tmp_path)

    assert cfg.show_decision_trace is False


def test_load_telegram_ui_config_reads_flag(tmp_path: Path) -> None:
    write_file(tmp_path, "settings.ini", """[telegram_ui]
show_decision_trace=true
""")
    write_file(tmp_path, "accounts.ini", """[acc]
login=u
password=p
host=h
""")

    cfg = load_telegram_ui_config(tmp_path)

    assert cfg.show_decision_trace is True


def test_load_web_ui_password_from_ini(tmp_path: Path) -> None:
    write_file(tmp_path, "settings.ini", """[web_ui]
password = ini-pass
""")
    write_file(tmp_path, "accounts.ini", """[acc]
login=u
password=p
host=h
""")

    password = load_web_ui_password_from_ini(tmp_path)

    assert password == "ini-pass"
