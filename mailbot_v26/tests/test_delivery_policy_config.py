from __future__ import annotations

from pathlib import Path

from mailbot_v26.config.delivery_policy import load_delivery_policy_config
from mailbot_v26.features.flags import FeatureFlags


def _write_config(tmp_path: Path, content: str) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.ini"
    config_path.write_text(content, encoding="utf-8")
    return config_dir


def test_load_delivery_policy_config_ignores_banned_keys(tmp_path: Path) -> None:
    config_dir = _write_config(
        tmp_path,
        """
[features]

[delivery_policy]
immediate_value_threshold = 70
critical_risk_threshold = 90
""".strip(),
    )
    cfg = load_delivery_policy_config(config_dir)
    assert cfg.immediate_value_threshold == 70
    assert cfg.critical_risk_threshold == 90
    assert cfg.max_immediate_per_hour == 0


def test_feature_flags_shadow_modes(tmp_path: Path) -> None:
    config_dir = _write_config(
        tmp_path,
        """
[features]
enable_circadian_delivery = true
enable_flow_protection = true
enable_attention_debt = true
enable_surprise_budget = shadow
enable_silence_as_signal = shadow
enable_deadlock_detection = shadow
""".strip(),
    )
    flags = FeatureFlags(base_dir=config_dir)
    assert flags.ENABLE_CIRCADIAN_DELIVERY is True
    assert flags.ENABLE_FLOW_PROTECTION is True
    assert flags.ENABLE_ATTENTION_DEBT is True
    assert flags.ENABLE_SURPRISE_BUDGET == "shadow"
    assert flags.ENABLE_SILENCE_AS_SIGNAL == "shadow"
    assert flags.ENABLE_DEADLOCK_DETECTION == "shadow"
