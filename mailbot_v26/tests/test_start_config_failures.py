from __future__ import annotations

import mailbot_v26.start as start_module


def test_load_config_missing_ini_uses_defaults_without_traceback(
    tmp_path, capsys
) -> None:
    _path, _raw, config = start_module.load_config(tmp_path)

    output = capsys.readouterr().out
    assert config.general.check_interval == 120
    assert "deterministic defaults" in output.lower()
    assert "Traceback" not in output


def test_load_config_invalid_config_ini_does_not_print_traceback(
    tmp_path, capsys
) -> None:
    (tmp_path / "config.ini").write_text("broken=1\n", encoding="utf-8")
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8"
    )
    (tmp_path / "keys.ini").write_text(
        "[telegram]\nbot_token=t\n[cloudflare]\naccount_id=a\napi_token=k\n",
        encoding="utf-8",
    )

    _path, _raw, config = start_module.load_config(tmp_path)

    output = capsys.readouterr().out
    assert config.general.check_interval == 120
    assert "Traceback" not in output


def test_start_yaml_windows_backslash_error_shows_actionable_hint_and_no_traceback(
    tmp_path,
    capsys,
) -> None:
    config_dir = tmp_path
    config_path = config_dir / "config.yaml"
    config_path.write_text('username: "HQ\\MedvedevSS"\n', encoding="utf-8")
    (config_dir / "settings.ini").write_text("[general]\n", encoding="utf-8")
    (config_dir / "accounts.ini").write_text(
        "[acc]\nlogin=user@example.com\npassword=p\nhost=h\n", encoding="utf-8"
    )

    _raw, _config = start_module._load_yaml_config_or_defaults(config_path, config_dir)

    output = capsys.readouterr().out
    assert "Use single quotes for Windows usernames/paths" in output
    assert "Traceback" not in output


def test_load_config_with_two_file_mode_only(tmp_path, capsys) -> None:
    (tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval=120\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u@example.com\npassword=p\nhost=imap.example.com\ntelegram_chat_id=1\n\n"
        "[telegram]\nbot_token=t\n\n"
        "[cloudflare]\naccount_id=a\napi_token=k\n",
        encoding="utf-8",
    )

    _path, _raw, config = start_module.load_config(tmp_path)

    output = capsys.readouterr().out
    assert config.general.check_interval == 120
    assert "config.yaml not found" not in output
    assert "Traceback" not in output


def test_load_config_with_explicit_config_dir_ignores_cwd_yaml(
    tmp_path, monkeypatch, capsys
) -> None:
    config_dir = tmp_path / "cfg"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        "[general]\ncheck_interval=120\n", encoding="utf-8"
    )
    (config_dir / "accounts.ini").write_text(
        "[acc]\nlogin=u@example.com\npassword=p\nhost=imap.example.com\ntelegram_chat_id=1\n",
        encoding="utf-8",
    )

    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (cwd / "config.yaml").write_text("not: [valid", encoding="utf-8")

    monkeypatch.chdir(cwd)

    _path, _raw, config = start_module.load_config(config_dir)

    output = capsys.readouterr().out
    assert config.general.check_interval == 120
    assert "config.yaml" not in output
    assert "Traceback" not in output


def test_start_fails_fast_on_invalid_telegram_contract(
    tmp_path, monkeypatch, capsys
) -> None:
    (tmp_path / "settings.ini").write_text(
        "[general]\ncheck_interval=120\n", encoding="utf-8"
    )
    (tmp_path / "accounts.ini").write_text(
        "[acc]\nlogin=u@example.com\npassword=p\nhost=imap.example.com\n\n"
        "[telegram]\nbot_token=CHANGE_ME\nchat_id==272123\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        start_module, "require_runtime_for", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        start_module, "validate_dist_runtime", lambda **kwargs: (True, "")
    )
    monkeypatch.setattr(start_module, "_check_build_integrity", lambda: None)

    try:
        start_module.main(config_dir=tmp_path, max_cycles=0)
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError(
            "Expected startup to fail-fast with invalid telegram config"
        )

    output = capsys.readouterr().out
    assert "Telegram configuration is invalid" in output
    assert "accounts.ini" in output
