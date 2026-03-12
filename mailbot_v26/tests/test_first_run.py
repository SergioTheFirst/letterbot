from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
from mailbot_v26.config_loader import load_config
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.tools.config_bootstrap import init_config, run_config_ready


def _write_valid_two_file_config(base_dir: Path) -> None:
    (base_dir / "settings.ini").write_text(
        "[general]\ncheck_interval = 120\n[storage]\ndb_path = data/mailbot.sqlite\n",
        encoding="utf-8",
    )
    (base_dir / "accounts.ini").write_text(
        "\n".join(
            [
                "[work]",
                "login = user@example.com",
                "password = secret-pass",
                "host = imap.example.com",
                "port = 993",
                "use_ssl = true",
                "telegram_chat_id = 123",
                "",
                "[telegram]",
                "bot_token = tg-token",
                "chat_id = 123",
            ]
        ),
        encoding="utf-8",
    )


def test_first_run_missing_config_gives_clear_message(tmp_path: Path, capsys) -> None:
    ready, critical, warnings = start_module._run_startup_preflight(tmp_path)  # noqa: SLF001

    start_module._print_startup_preflight_failure(tmp_path, critical, warnings)  # noqa: SLF001

    output = capsys.readouterr().out
    assert ready is False
    assert "Missing settings.ini" in output
    assert "Missing accounts.ini" in output
    assert "init-config" in output


def test_first_run_no_config_creates_accounts_ini(tmp_path: Path) -> None:
    result = init_config(tmp_path)

    settings_path = tmp_path / "settings.ini"
    accounts_path = tmp_path / "accounts.ini"

    assert settings_path.exists()
    assert accounts_path.exists()
    assert settings_path in result["created"]
    assert accounts_path in result["created"]
    assert "CHANGE_ME" in settings_path.read_text(encoding="utf-8")
    assert "CHANGE_ME" in accounts_path.read_text(encoding="utf-8")


def test_first_run_config_ready_fails_on_placeholder_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    init_config(tmp_path)

    exit_code = run_config_ready(tmp_path, verbose=True)
    output = capsys.readouterr().out

    assert exit_code == 2
    assert "STATUS: NOT_READY" in output
    assert "bootstrap template" in output
    assert "No ready IMAP account found" in output
    assert "bot_token" in output


def test_first_run_config_ready_passes_after_filling_credentials(tmp_path: Path) -> None:
    _write_valid_two_file_config(tmp_path)

    assert run_config_ready(tmp_path, verbose=False) == 0


def test_init_config_idempotent_second_run_does_not_overwrite(tmp_path: Path) -> None:
    init_config(tmp_path)
    accounts_path = tmp_path / "accounts.ini"
    settings_path = tmp_path / "settings.ini"

    accounts_path.write_text(
        "[work]\nlogin = real@example.com\npassword = secret\nhost = imap.example.com\nport = 993\nuse_ssl = true\n",
        encoding="utf-8",
    )
    settings_path.write_text("[general]\ncheck_interval = 42\n", encoding="utf-8")

    result = init_config(tmp_path)

    assert result["created"] == []
    assert "real@example.com" in accounts_path.read_text(encoding="utf-8")
    assert "check_interval = 42" in settings_path.read_text(encoding="utf-8")


def test_init_config_works_from_arbitrary_path(tmp_path: Path) -> None:
    target_dir = tmp_path / "первый запуск с пробелом"

    run_result = init_config(target_dir)

    assert (target_dir / "settings.ini").exists()
    assert (target_dir / "accounts.ini").exists()
    assert len(run_result["created"]) == 2


def test_config_ready_output_is_human_readable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    init_config(tmp_path)

    exit_code = run_config_ready(tmp_path, verbose=True)
    output = capsys.readouterr().out

    assert exit_code == 2
    assert "Traceback" not in output
    assert "ConfigParser" not in output
    assert "bootstrap template" in output
    assert "bot_token" in output


def test_first_run_placeholder_credentials_give_clear_message(
    tmp_path: Path, capsys
) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval=120\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        "\n".join(
            [
                "[work]",
                "login = user@example.com",
                "password = CHANGE_ME",
                "host = CHANGE_ME",
                "port = 993",
                "use_ssl = true",
            ]
        ),
        encoding="utf-8",
    )

    ready, critical, warnings = start_module._run_startup_preflight(tmp_path)  # noqa: SLF001
    start_module._print_startup_preflight_failure(tmp_path, critical, warnings)  # noqa: SLF001

    output = capsys.readouterr().out
    assert ready is False
    assert "missing required fields" in output
    assert "password" in output
    assert "host" in output
    assert "Traceback" not in output


def test_first_run_example_account_requires_rename(tmp_path: Path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval=120\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        "\n".join(
            [
                "[example_account]",
                "login = user@example.com",
                "password = secret-pass",
                "host = imap.example.com",
                "port = 993",
                "use_ssl = true",
                "telegram_chat_id = 123",
                "",
                "[telegram]",
                "bot_token = tg-token",
                "chat_id = 123",
            ]
        ),
        encoding="utf-8",
    )

    ready, critical, warnings = start_module._run_startup_preflight(tmp_path)  # noqa: SLF001
    start_module._print_startup_preflight_failure(tmp_path, critical, warnings)  # noqa: SLF001

    output = capsys.readouterr().out
    assert ready is False
    assert any("bootstrap template" in item for item in critical)
    assert "rename the section" in output
    assert warnings == []


def test_first_run_valid_config_returns_true(tmp_path: Path) -> None:
    _write_valid_two_file_config(tmp_path)

    ready, critical, warnings = start_module._run_startup_preflight(tmp_path)  # noqa: SLF001

    assert ready is True
    assert critical == []
    assert warnings == []


def test_startup_confirmation_has_no_credentials(tmp_path: Path) -> None:
    _write_valid_two_file_config(tmp_path)
    config = load_config(tmp_path)
    resolved = resolve_config_paths(tmp_path)

    lines = start_module._build_startup_confirmation_lines(  # noqa: SLF001
        config=config,
        resolved_config_dir=tmp_path,
        two_file_mode=resolved.two_file_mode,
    )
    text = "\n".join(lines)

    assert "secret-pass" not in text
    assert "user@example.com" not in text
    assert "imap.example.com" not in text
    assert "mailbot.sqlite" in text
    assert "mailbot.log" in text


def test_db_dir_created_automatically_if_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "data" / "mailbot.sqlite"
    log_path = tmp_path / "logs" / "mailbot.log"

    start_module._ensure_runtime_dirs(db_path=db_path, log_path=log_path)  # noqa: SLF001

    assert db_path.parent.exists()


def test_logs_dir_created_automatically_if_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "mailbot.sqlite"
    log_path = tmp_path / "nested" / "logs" / "mailbot.log"

    start_module._ensure_runtime_dirs(db_path=db_path, log_path=log_path)  # noqa: SLF001

    assert log_path.parent.exists()


def test_graceful_exit_on_missing_config_no_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setattr(
        start_module, "require_runtime_for", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        start_module, "validate_dist_runtime", lambda **_kwargs: (True, "")
    )
    monkeypatch.setattr(start_module, "_check_build_integrity", lambda: None)
    monkeypatch.setattr(
        start_module.processor_module,
        "system_snapshotter",
        SimpleNamespace(log_startup=lambda: None),
    )

    with pytest.raises(SystemExit) as exc_info:
        start_module.main(config_dir=tmp_path, max_cycles=0)

    output = capsys.readouterr().out
    assert exc_info.value.code == 2
    assert "Configuration is not ready for startup." in output
    assert "Traceback" not in output
