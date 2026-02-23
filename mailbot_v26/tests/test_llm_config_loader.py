from __future__ import annotations

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
