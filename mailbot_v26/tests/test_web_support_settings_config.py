from __future__ import annotations

from pathlib import Path

from mailbot_v26.web_observability.app import _load_support_settings


def _write_config(tmp_path: Path, payload: str) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(payload, encoding="utf-8")
    return config_path


def _write_settings(tmp_path: Path, payload: str) -> Path:
    settings_path = tmp_path / "settings.ini"
    settings_path.write_text(payload, encoding="utf-8")
    return settings_path


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


def test_load_support_settings_support_switch_overrides_legacy_feature(
    tmp_path: Path,
) -> None:
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


def test_load_support_settings_uses_ini_when_yaml_missing(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        """
[support]
enabled = true
show_in_nav = true
label = Поддержать Letterbot
text = Поддержать проект можно по ссылке или QR-коду
url = https://example.com/support
details = Boosty / СБП
qr_image = support.png
""",
    )
    (tmp_path / "support.png").write_bytes(
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc````\x00\x00\x00\x04\x00\x01\x0b\xe7\x02\x9d\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    settings = _load_support_settings(tmp_path / "config.yaml")

    assert settings.enabled is True
    assert settings.show_in_nav is True
    assert settings.text == "Поддержать проект можно по ссылке или QR-коду"
    assert len(settings.methods) == 1
    assert settings.methods[0].label == "Поддержать Letterbot"
    assert settings.methods[0].url == "https://example.com/support"
    assert settings.methods[0].qr_image_data_uri.startswith("data:image/png;base64,")


def test_load_support_settings_ini_master_switch_off(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        """
[support]
enabled = false
show_in_nav = true
label = Поддержать Letterbot
""",
    )

    settings = _load_support_settings(tmp_path / "config.yaml")

    assert settings.enabled is False
    assert settings.methods == []


def test_load_support_settings_ini_nav_hidden_but_page_enabled(tmp_path: Path) -> None:
    _write_settings(
        tmp_path,
        """
[support]
enabled = true
show_in_nav = false
label = Поддержать Letterbot
url = https://example.com/support
""",
    )

    settings = _load_support_settings(tmp_path / "config.yaml")

    assert settings.enabled is True
    assert settings.show_in_nav is False
    assert len(settings.methods) == 1
