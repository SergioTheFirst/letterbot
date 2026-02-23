from __future__ import annotations

import configparser
import importlib
import logging
import sys

from mailbot_v26.features.flags import FeatureFlags


def test_importing_pipeline_processor_does_not_read_ini_on_import(monkeypatch) -> None:
    def _fail_read(*_args, **_kwargs):
        raise AssertionError("ConfigParser.read must not be called during import")

    monkeypatch.setattr(configparser.ConfigParser, "read", _fail_read)
    sys.modules.pop("mailbot_v26.pipeline.processor", None)
    importlib.import_module("mailbot_v26.pipeline.processor")


def test_malformed_config_ini_logs_single_actionable_warning(tmp_path, caplog) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "config.ini").write_text("check_interval=120\n", encoding="utf-8")

    caplog.set_level(logging.WARNING)
    flags = FeatureFlags(base_dir=config_dir)

    assert flags.ENABLE_AUTO_PRIORITY is False
    warnings = [
        record.getMessage()
        for record in caplog.records
        if "config.ini is invalid" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert "Template:" in warnings[0]
    assert "Windows command: copy" in warnings[0]
