from __future__ import annotations

from types import SimpleNamespace

import mailbot_v26.start as start_module


def test_start_cli_missing_required_ini_exits_with_clear_message(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    monkeypatch.setattr(start_module, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(start_module, "validate_dist_runtime", lambda **_kwargs: (True, ""))
    monkeypatch.setattr(start_module, "_check_build_integrity", lambda: None)
    monkeypatch.setattr(start_module.processor_module, "system_snapshotter", SimpleNamespace(log_startup=lambda: None))

    exit_code = start_module.main_cli(["--config-dir", str(tmp_path), "--max-cycles", "1"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "INI configuration invalid" in output
    assert "mailbot_v26/config/config.ini" in output


def test_start_cli_invalid_config_ini_exits_without_traceback(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    config_dir = tmp_path
    (config_dir / "config.ini").write_text("broken=1\n", encoding="utf-8")
    (config_dir / "accounts.ini").write_text("[acc]\nlogin=u\npassword=p\nhost=h\n", encoding="utf-8")
    (config_dir / "keys.ini").write_text(
        "[telegram]\nbot_token=t\n[cloudflare]\naccount_id=a\napi_token=k\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(start_module, "require_runtime_for", lambda _mode: None)
    monkeypatch.setattr(start_module, "validate_dist_runtime", lambda **_kwargs: (True, ""))
    monkeypatch.setattr(start_module, "_check_build_integrity", lambda: None)
    monkeypatch.setattr(start_module.processor_module, "system_snapshotter", SimpleNamespace(log_startup=lambda: None))

    exit_code = start_module.main_cli(["--config-dir", str(config_dir), "--max-cycles", "1"])

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "INI configuration invalid" in output
    assert "Traceback" not in output
