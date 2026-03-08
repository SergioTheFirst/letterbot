import sqlite3
from pathlib import Path

import pytest

CYRILLIC_DIR_NAME = "папка с пробелами"


@pytest.fixture
def cyrillic_tmp_path(tmp_path: Path) -> Path:
    """Временная директория с кириллицей и пробелами в имени."""
    directory = tmp_path / CYRILLIC_DIR_NAME / "sub dir"
    directory.mkdir(parents=True)
    return directory


def test_settings_ini_read_from_cyrillic_path(cyrillic_tmp_path: Path) -> None:
    settings = cyrillic_tmp_path / "settings.ini"
    settings.write_text("[general]\ncheck_interval = 60\n", encoding="utf-8")

    from mailbot_v26.config.ini_utils import read_user_ini_with_defaults

    parser = read_user_ini_with_defaults(settings)
    assert parser.get("general", "check_interval") == "60"


def test_accounts_ini_read_from_cyrillic_path(cyrillic_tmp_path: Path) -> None:
    accounts = cyrillic_tmp_path / "accounts.ini"
    accounts.write_text(
        "[telegram]\n"
        "bot_token = 123:abc\n"
        "chat_id = 456\n"
        "[mos_ru]\n"
        "login = user\n"
        "password = pass\n"
        "host = mail.ru\n"
        "port = 993\n"
        "use_ssl = true\n"
        "telegram_chat_id = 456\n",
        encoding="utf-8",
    )

    from mailbot_v26.config_loader import load_accounts_config, load_keys_config

    keys = load_keys_config(cyrillic_tmp_path)
    accounts_cfg = load_accounts_config(cyrillic_tmp_path)
    assert keys.telegram_bot_token == "123:abc"
    assert len(accounts_cfg) == 1


def test_sqlite_db_in_cyrillic_path(cyrillic_tmp_path: Path) -> None:
    db_path = cyrillic_tmp_path / "mailbot.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    assert db_path.exists()


def test_storage_class_in_cyrillic_path(cyrillic_tmp_path: Path) -> None:
    from mailbot_v26.bot_core.storage import Storage

    db_path = cyrillic_tmp_path / "data" / "mailbot.sqlite"
    storage = Storage(db_path)
    storage.conn.execute("SELECT 1").fetchone()


def test_resolve_config_paths_with_spaces(cyrillic_tmp_path: Path) -> None:
    (cyrillic_tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval = 120\n", encoding="utf-8"
    )
    (cyrillic_tmp_path / "accounts.ini").write_text(
        "[telegram]\nbot_token = test:token\n", encoding="utf-8"
    )

    from mailbot_v26.config_loader import resolve_config_paths

    resolved = resolve_config_paths(cyrillic_tmp_path)
    assert resolved.settings_path.exists()
    assert resolved.accounts_path.exists()
    assert resolved.two_file_mode


def test_state_manager_in_cyrillic_path(cyrillic_tmp_path: Path) -> None:
    from mailbot_v26.state_manager import StateManager

    state_file = cyrillic_tmp_path / "state.json"
    sm = StateManager(state_file)
    sm.update_last_uid("test@example.com", 42)
    sm.save()

    sm2 = StateManager(state_file)
    assert sm2.get_last_uid("test@example.com") == 42


def test_run_stack_build_worker_command_with_cyrillic_path(tmp_path: Path) -> None:
    from mailbot_v26.tools.run_stack import _build_worker_command

    cyrillic_path = tmp_path / "папка Иванова" / "letterbot"
    cyrillic_path.mkdir(parents=True)

    cmd = _build_worker_command("python", cyrillic_path)
    script = cmd.args[2]
    assert repr(str(cyrillic_path.resolve())) in script
