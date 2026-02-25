from __future__ import annotations

import logging
from pathlib import Path

from mailbot_v26.llm.router import _load_llm_config


def test_llm_loader_handles_settings_without_llm_safety(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "settings.ini").write_text(
        "[llm]\nprimary=cloudflare\nfallback=cloudflare\n[cloudflare]\nenabled=true\n",
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "[telegram]\nbot_token=t\n[cloudflare]\naccount_id=a\napi_token=k\n",
        encoding="utf-8",
    )

    loaded = _load_llm_config(config_dir)

    assert loaded.cloudflare_enabled is True
    assert loaded.gigachat_max_consecutive_errors == 3
    assert loaded.gigachat_max_latency_sec == 10
    assert loaded.gigachat_cooldown_sec == 600


def test_llm_loader_reads_cloudflare_secrets_from_accounts_in_two_file_mode(tmp_path: Path, caplog) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "settings.ini").write_text(
        "[llm]\nprimary=cloudflare\nfallback=cloudflare\n",
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "[cloudflare]\naccount_id=acc_id\napi_token=acc_token\n",
        encoding="utf-8",
    )

    caplog.set_level(logging.WARNING)
    loaded = _load_llm_config(config_dir)

    assert loaded.cloudflare_account_id == "acc_id"
    assert loaded.cloudflare_api_key == "acc_token"
    assert "keys.ini missing" not in caplog.text


def test_llm_loader_uses_accounts_llm_mapping_in_two_file_mode(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "settings.ini").write_text(
        "[llm]\nprimary=cloudflare\nfallback=cloudflare\n[gigachat]\nenabled=true\n",
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "[llm]\nprimary=gigachat\nfallback=cloudflare\n[gigachat]\napi_key=from_accounts\n",
        encoding="utf-8",
    )

    loaded = _load_llm_config(config_dir)

    assert loaded.primary == "gigachat"
    assert loaded.fallback == "cloudflare"
    assert loaded.gigachat_api_key == "from_accounts"


def test_llm_loader_keeps_legacy_keys_ini_when_not_in_two_file_mode(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "settings.ini").write_text(
        "[llm]\nprimary=cloudflare\nfallback=cloudflare\n",
        encoding="utf-8",
    )
    (config_dir / "keys.ini").write_text(
        "[cloudflare]\naccount_id=legacy_acc\napi_token=legacy_token\n",
        encoding="utf-8",
    )

    loaded = _load_llm_config(config_dir)

    assert loaded.primary == "cloudflare"
    assert loaded.cloudflare_account_id == "legacy_acc"
    assert loaded.cloudflare_api_key == "legacy_token"
