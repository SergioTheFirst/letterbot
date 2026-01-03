from __future__ import annotations

from pathlib import Path

from mailbot_v26.config.premium_clarity import load_premium_clarity_config


def _write_config(tmp_path: Path, content: str) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "config.ini"
    config_path.write_text(content, encoding="utf-8")
    return config_dir


def test_premium_clarity_config_defaults(tmp_path: Path) -> None:
    config_dir = _write_config(
        tmp_path,
        """
[features]
""".strip(),
    )
    cfg = load_premium_clarity_config(config_dir)
    assert cfg.confidence_dots_mode == "auto"
    assert cfg.confidence_dots_threshold == 75


def test_premium_clarity_config_parses_values(tmp_path: Path) -> None:
    config_dir = _write_config(
        tmp_path,
        """
[premium_clarity]
premium_clarity_confidence_dots = always
premium_clarity_confidence_threshold = 80
""".strip(),
    )
    cfg = load_premium_clarity_config(config_dir)
    assert cfg.confidence_dots_mode == "always"
    assert cfg.confidence_dots_threshold == 80
