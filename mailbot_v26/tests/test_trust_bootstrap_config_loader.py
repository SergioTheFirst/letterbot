from __future__ import annotations

from pathlib import Path

from mailbot_v26.config.trust_bootstrap import load_trust_bootstrap_config


def test_trust_bootstrap_prefers_settings_ini_section(tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text(
        """
[trust_bootstrap]
learning_days = 21
min_samples = 10
templates_min_corrections = 10
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "config.ini").write_text(
        """
[trust_bootstrap]
learning_days = 14
min_samples = 99
templates_min_corrections = 99
""".strip(),
        encoding="utf-8",
    )

    cfg = load_trust_bootstrap_config(tmp_path)

    assert cfg.learning_days == 21
    assert cfg.min_samples == 10
    assert cfg.templates_min_corrections == 10


def test_trust_bootstrap_falls_back_to_legacy_config_ini(tmp_path: Path) -> None:
    (tmp_path / "settings.ini").write_text("[general]\ncheck_interval = 120\n", encoding="utf-8")
    (tmp_path / "config.ini").write_text(
        """
[trust_bootstrap]
min_samples = 10
templates_min_corrections = 10
""".strip(),
        encoding="utf-8",
    )

    cfg = load_trust_bootstrap_config(tmp_path)

    assert cfg.min_samples == 10
    assert cfg.templates_min_corrections == 10


def test_trust_bootstrap_defaults_when_both_files_missing(tmp_path: Path) -> None:
    cfg = load_trust_bootstrap_config(tmp_path)

    assert cfg.learning_days == 14
    assert cfg.min_samples == 50
    assert cfg.templates_min_corrections == 20
