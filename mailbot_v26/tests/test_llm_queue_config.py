from __future__ import annotations

from mailbot_v26.config.llm_queue import load_llm_queue_config


def test_llm_queue_defaults_disabled(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        "[general]\ncheck_interval = 120\n", encoding="utf-8"
    )

    config = load_llm_queue_config(config_dir)

    assert config.llm_request_queue_enabled is False
    assert config.max_concurrent_llm_calls == 1
