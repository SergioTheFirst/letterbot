from __future__ import annotations

from pathlib import Path

from mailbot_v26.web_observability.app import _load_support_settings


def _write_config(tmp_path: Path, payload: str) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(payload, encoding="utf-8")
    return config_path


def test_load_support_settings_enabled_via_support_switch(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
support:
  enabled: true
  ui:
    show_in_nav: true
  methods:
    - type: card
      label: Карта
      number: "2202"
""",
    )

    settings = _load_support_settings(config_path)

    assert settings.enabled is True
    assert settings.show_in_nav is True
    assert len(settings.methods) == 1


def test_load_support_settings_support_switch_overrides_legacy_feature(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
features:
  donate_enabled: true
support:
  enabled: false
  ui:
    show_in_nav: true
  methods:
    - type: card
      label: Карта
      number: "2202"
""",
    )

    settings = _load_support_settings(config_path)

    assert settings.enabled is False
