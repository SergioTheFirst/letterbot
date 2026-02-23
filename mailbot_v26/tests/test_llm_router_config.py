from __future__ import annotations

from mailbot_v26.llm import router


def test_load_llm_config_missing_files_returns_degraded_defaults(tmp_path) -> None:
    config = router._load_llm_config(tmp_path)

    assert config is not None
    assert config.cloudflare_enabled is True
    assert config.cloudflare_account_id == ""
    assert config.cloudflare_api_key == ""
