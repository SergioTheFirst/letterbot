from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.config.delivery_policy import DeliveryPolicyConfig
from mailbot_v26.config.flow_protection import load_flow_protection_config
from mailbot_v26.pipeline.processor import _build_delivery_context


def _write_config(tmp_path: Path, content: str) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.ini"
    config_path.write_text(content, encoding="utf-8")
    return config_dir


def test_flow_protection_context_parses_focus_hours(tmp_path: Path) -> None:
    config_dir = _write_config(
        tmp_path,
        """
[features]
enable_flow_protection = true

[flow_protection]
focus_hours = 8-10
""".strip(),
    )
    flow_config = load_flow_protection_config(config_dir)
    assert flow_config.focus_start_hour == 8
    assert flow_config.focus_end_hour == 10

    now_local = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)
    context = _build_delivery_context(
        now_local=now_local,
        policy_config=DeliveryPolicyConfig(),
        flow_config=flow_config,
        enable_circadian=False,
        enable_flow_protection=True,
        immediate_sent_last_hour=0,
    )
    assert context.is_focus_hours is True
