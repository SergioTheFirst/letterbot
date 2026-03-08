from __future__ import annotations

import configparser
import importlib
import sys
from pathlib import Path

from mailbot_v26.config.auto_priority_gate import load_auto_priority_gate_config


def test_malformed_ini_returns_defaults_and_warning(tmp_path: Path, caplog) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text("[broken\nenabled = true\n", encoding="utf-8")

    caplog.set_level("WARNING")
    cfg = load_auto_priority_gate_config(tmp_path)

    assert cfg.enabled is True
    assert cfg.window_days == 30
    assert cfg.min_samples == 10
    assert any(
        "using deterministic defaults" in rec.message.lower() for rec in caplog.records
    )


def test_legacy_no_section_ini_parses_without_crash(tmp_path: Path) -> None:
    config_path = tmp_path / "config.ini"
    config_path.write_text(
        "enabled = true ' legacy inline comment\n"
        "window_days = 14\n"
        "min_samples = 9\n"
        "max_correction_rate = 0.22\n"
        "cooldown_hours = 12\n",
        encoding="utf-8",
    )

    cfg = load_auto_priority_gate_config(tmp_path)

    assert cfg.enabled is True
    assert cfg.window_days == 14
    assert cfg.min_samples == 9
    assert cfg.max_correction_rate == 0.22
    assert cfg.cooldown_hours == 12


def test_import_processor_does_not_read_ini_on_import(monkeypatch) -> None:
    module_name = "mailbot_v26.pipeline.processor"
    sys.modules.pop(module_name, None)

    def _fail_read(self, *_args, **_kwargs):
        raise AssertionError("ConfigParser.read must not be called during import")

    monkeypatch.setattr(configparser.ConfigParser, "read", _fail_read)

    importlib.import_module(module_name)


def test_auto_priority_gate_loader_defaults(tmp_path: Path) -> None:
    cfg = load_auto_priority_gate_config(tmp_path)

    assert cfg.enabled is True
    assert cfg.min_samples == 10
