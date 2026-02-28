from __future__ import annotations

import configparser
from pathlib import Path

from mailbot_v26.config_loader import load_general_config
from mailbot_v26.tools.config_bootstrap import (
    SETTINGS_TEMPLATE,
    check_config_ready,
    migrate_two_file_config,
    init_config,
    run_config_ready,
    validate_config,
)


def test_init_config_keeps_placeholder_examples(tmp_path) -> None:
    result = init_config(tmp_path)

    accounts_file = tmp_path / "accounts.ini"
    assert accounts_file.exists(), "accounts.ini must be created by init_config"
    assert result["created"]

    content = accounts_file.read_text(encoding="utf-8")
    assert "CHANGE_ME" in content
    assert "HQ\\MedvedevSS" in content


def test_config_ready_ignores_optional_change_me_sections(tmp_path) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        """
[work]
login = HQ\\MedvedevSS
password = fake-pass
host = imap.example.com
port = 993
use_ssl = true

[telegram]
bot_token = CHANGE_ME

[cloudflare]
api_token = CHANGE_ME
account_id = CHANGE_ME
; CHANGE_ME in comment must not block readiness
""".strip(),
        encoding="utf-8",
    )

    ready, critical, warnings = check_config_ready(tmp_path)

    assert ready is True
    assert critical == []
    assert warnings
    assert run_config_ready(tmp_path, verbose=False) == 0


def test_validate_config_treats_system_sections_by_own_rules(tmp_path) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        """
[acc]
login = u
password = p
host = h
port = 993
use_ssl = true
telegram_chat_id =

[telegram]
bot_token = tg

[cloudflare]
account_id = a
api_token = b

[gigachat]
api_key = c

[llm]
primary = cloudflare
fallback = gigachat
""".strip(),
        encoding="utf-8",
    )

    ok, issues = validate_config(tmp_path)

    assert ok is False
    assert any("telegram_chat_id is recommended" in issue for issue in issues)
    assert not any("[telegram] host is required" in issue for issue in issues)
    assert not any("[cloudflare] login is required" in issue for issue in issues)
    assert not any("[gigachat] use_ssl is required" in issue for issue in issues)
    assert not any("[llm] host is required" in issue for issue in issues)


def test_two_file_mode_does_not_require_yaml_or_keys_for_readiness(tmp_path) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        """
[acc]
login = user@example.com
password = pass
host = imap.example.com
port = 993
use_ssl = true
""".strip(),
        encoding="utf-8",
    )

    assert run_config_ready(tmp_path, verbose=False) == 0


def test_settings_example_matches_settings_template() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    example_path = repo_root / "mailbot_v26" / "config" / "settings.ini.example"
    example_text = example_path.read_text(encoding="utf-8")

    assert example_text == SETTINGS_TEMPLATE


def test_settings_example_parses_as_ints_and_booleans() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    example_path = repo_root / "mailbot_v26" / "config" / "settings.ini.example"

    parser = configparser.ConfigParser()
    parser.read(example_path, encoding="utf-8")

    assert parser.getint("general", "check_interval") == 120
    assert parser.getboolean("features", "enable_premium_processor") is True
    assert parser.getint("weekly_calibration_report", "top_n") == 3
    assert parser.has_section("support")
    assert parser.getint("support", "frequency_days") == 30


def test_settings_example_contains_runtime_sections_and_no_inline_comments() -> None:
    parser = configparser.ConfigParser()
    parser.read_string(SETTINGS_TEMPLATE)

    required_sections = {"general", "features", "web", "delivery_policy", "silence_policy", "deadlock_policy", "support"}
    assert required_sections.issubset(set(parser.sections()))

    for section in parser.sections():
        for key, value in parser.items(section):
            assert " ;" not in value, f"{section}.{key} contains inline ';' comment tail"
            assert " #" not in value, f"{section}.{key} contains inline '#' comment tail"


def test_two_file_mode_defaults_work_without_settings_ini(tmp_path) -> None:
    (tmp_path / "accounts.ini").write_text(
        """
[acc]
login = user@example.com
password = pass
host = imap.example.com
port = 993
use_ssl = true
""".strip(),
        encoding="utf-8",
    )

    general = load_general_config(tmp_path)

    assert general.check_interval == 120
    assert general.max_email_mb == 15
    assert general.max_extracted_total_chars == 120000


def test_migrate_config_creates_settings_with_runtime_sections(tmp_path) -> None:
    (tmp_path / "accounts.ini").write_text(
        """
[acc]
login = user@example.com
password = pass
host = imap.example.com
port = 993
use_ssl = true
""".strip(),
        encoding="utf-8",
    )

    result = migrate_two_file_config(tmp_path)
    assert tmp_path / "settings.ini" in result["created"]

    parser = configparser.ConfigParser()
    parser.read(tmp_path / "settings.ini", encoding="utf-8")

    for section in ("general", "features", "web", "delivery_policy"):
        assert parser.has_section(section)
