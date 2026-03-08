import pytest

pytest.importorskip("yaml")

from mailbot_v26.config_yaml import (
    ConfigError,
    load_config,
    resolve_support_enabled,
    validate_config,
)


def _base_config() -> dict:
    return {
        "telegram": {"bot_token": "token", "chat_id": "123"},
        "llm": {
            "provider": "cloudflare",
            "cloudflare": {"api_token": "cf-token", "account_id": "acc", "model": None},
            "gigachat": {"api_token": "giga-token", "model": None},
        },
        "accounts": [
            {
                "name": "Work",
                "email": "work@example.com",
                "imap_host": "imap.example.com",
                "imap_port": 993,
                "username": "work@example.com",
                "password": "pass",
                "enabled": True,
            }
        ],
        "polling": {"interval_seconds": 60, "reload_config_seconds": 60},
        "web_ui": {
            "enabled": True,
            "bind": "127.0.0.1",
            "port": 8787,
            "password": "pw",
            "api_token": "token",
            "allow_lan": False,
            "allow_cidrs": ["192.168.0.0/16"],
            "prod_server": False,
            "require_strong_password_on_lan": True,
        },
    }


def test_validate_config_success() -> None:
    ok, error = validate_config(_base_config())

    assert ok
    assert error == ""


def test_validate_config_missing_accounts() -> None:
    config = _base_config()
    config.pop("accounts", None)

    ok, error = validate_config(config)

    assert not ok
    assert error == "Ошибка в config.yaml: accounts отсутствует"


def test_validate_config_invalid_provider() -> None:
    config = _base_config()
    config["llm"]["provider"] = "unknown"

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == 'Ошибка в config.yaml: llm.provider должен быть "cloudflare" или "gigachat"'
    )


def test_validate_config_model_null_allowed() -> None:
    config = _base_config()
    config["llm"]["cloudflare"]["model"] = None

    ok, error = validate_config(config)

    assert ok
    assert error == ""


def test_validate_config_default_local_ok() -> None:
    config = _base_config()
    config["web_ui"]["bind"] = "127.0.0.1"
    config["web_ui"]["allow_lan"] = False

    ok, error = validate_config(config)

    assert ok
    assert error == ""


def test_validate_config_lan_requires_allowlist() -> None:
    config = _base_config()
    config["web_ui"]["bind"] = "0.0.0.0"
    config["web_ui"]["allow_lan"] = False

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: web_ui.allow_lan должен быть true для bind вне loopback"
    )

    config["web_ui"]["allow_lan"] = True
    config["web_ui"]["allow_cidrs"] = []

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: web_ui.allow_cidrs должен быть непустым при allow_lan=true"
    )


def test_validate_config_lan_password_policy() -> None:
    config = _base_config()
    config["web_ui"]["bind"] = "0.0.0.0"
    config["web_ui"]["allow_lan"] = True
    config["web_ui"]["allow_cidrs"] = ["192.168.0.0/16"]
    config["web_ui"]["password"] = "CHANGE_ME"

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: web_ui.password не должен быть значением по умолчанию для LAN"
    )

    config["web_ui"]["password"] = "short"
    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: web_ui.password должен быть не короче 10 символов для LAN"
    )


def test_validate_config_support_card_requires_number() -> None:
    config = _base_config()
    config["support"] = {
        "enabled": True,
        "methods": [{"type": "card", "label": "Card", "number": ""}],
        "telegram": {"enabled": False, "frequency_days": 30, "text": ""},
    }

    ok, error = validate_config(config)

    assert not ok
    assert error == "Ошибка в config.yaml: support.methods[0].number отсутствует"


def test_validate_config_support_telegram_requires_text_when_enabled() -> None:
    config = _base_config()
    config["support"] = {
        "enabled": True,
        "methods": [{"type": "card", "label": "Card", "number": "2200"}],
        "telegram": {"enabled": True, "frequency_days": 30, "text": ""},
    }

    ok, error = validate_config(config)

    assert not ok
    assert error == "Ошибка в config.yaml: support.telegram.text отсутствует"


def test_validate_config_support_frequency_range() -> None:
    config = _base_config()
    config["support"] = {
        "enabled": True,
        "methods": [{"type": "card", "label": "Card", "number": "2200"}],
        "telegram": {"enabled": True, "frequency_days": 3, "text": "Support"},
    }

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: support.telegram.frequency_days должен быть числом 7..365"
    )


def test_validate_config_support_yoomoney_url_scheme() -> None:
    config = _base_config()
    config["support"] = {
        "enabled": True,
        "methods": [{"type": "yoomoney", "label": "YM", "url": "ftp://bad"}],
        "telegram": {"enabled": False, "frequency_days": 30, "text": ""},
    }

    ok, error = validate_config(config)

    assert not ok
    assert (
        error
        == "Ошибка в config.yaml: support.methods[0].url должен начинаться с http(s)"
    )


def test_validate_config_support_success() -> None:
    config = _base_config()
    config["support"] = {
        "enabled": True,
        "ui": {"show_in_nav": True},
        "methods": [
            {
                "type": "sbp",
                "label": "СБП",
                "details": "По телефону",
                "phone": "+79990001122",
                "qr_image": "assets/sbp_qr.png",
            },
            {"type": "card", "label": "Карта", "number": "2202 2000"},
            {"type": "yoomoney", "label": "YM", "url": "https://yoomoney.ru/to/123"},
        ],
        "telegram": {
            "enabled": True,
            "frequency_days": 30,
            "text": "Letterbot бесплатный. Поддержка: /support",
        },
    }

    ok, error = validate_config(config)

    assert ok
    assert error == ""


def test_validate_config_features_donate_enabled_default_off() -> None:
    config = _base_config()

    ok, error = validate_config(config)

    assert ok
    assert error == ""


def test_validate_config_features_donate_enabled_type() -> None:
    config = _base_config()
    config["features"] = {"donate_enabled": "yes"}

    ok, error = validate_config(config)

    assert not ok
    assert (
        error == "Ошибка в config.yaml: features.donate_enabled должен быть true/false"
    )


def test_resolve_support_enabled_prefers_support_enabled() -> None:
    config = _base_config()
    config["features"] = {"donate_enabled": False}
    config["support"] = {"enabled": True}

    assert resolve_support_enabled(config) is True


def test_resolve_support_enabled_falls_back_to_features_donate_enabled() -> None:
    config = _base_config()
    config["features"] = {"donate_enabled": True}

    assert resolve_support_enabled(config) is True


def test_resolve_support_enabled_support_disables_legacy_flag() -> None:
    config = _base_config()
    config["features"] = {"donate_enabled": True}
    config["support"] = {"enabled": False}

    assert resolve_support_enabled(config) is False


def test_load_config_windows_double_quoted_username_shows_actionable_hint(
    tmp_path,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text('username: "HQ\\MedvedevSS"\n', encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    message = str(exc_info.value)
    assert "Use single quotes for Windows usernames/paths" in message
    assert "escape backslashes as \\" in message
    assert "unknown escape" not in message.lower()


def test_load_config_windows_single_quoted_username_parses(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("username: 'HQ\\MedvedevSS'\n", encoding="utf-8")

    payload = load_config(config_path)

    assert payload["username"] == r"HQ\MedvedevSS"


def test_web_ui_disabled_requires_no_bind_or_port(tmp_path) -> None:
    """Regression: web_ui: {enabled: false} must pass validation without bind/port.
    This is the exact config a first-time user writes. Must not block startup.
    """
    import yaml
    from mailbot_v26.config_yaml import load_config, validate_config

    cfg = {
        "schema_version": 1,
        "telegram": {"bot_token": "123:tok", "chat_id": "12345"},
        "llm": {
            "provider": "cloudflare",
            "cloudflare": {"api_token": "tok", "account_id": "acc"},
            "gigachat": {"api_token": ""},
        },
        "accounts": [
            {
                "name": "Test",
                "email": "x@y.com",
                "imap_host": "imap.y.com",
                "imap_port": 993,
                "username": "x@y.com",
                "password": "pass",
                "enabled": True,
            }
        ],
        "polling": {"interval_seconds": 60, "reload_config_seconds": 300},
        "web_ui": {"enabled": False},  # No bind, no port — must be valid
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(cfg), encoding="utf-8")

    raw = load_config(config_path)
    ok, err = validate_config(raw)

    assert ok is True, f"Expected valid config, got error: {err}"


def test_web_ui_enabled_still_requires_bind_and_port(tmp_path) -> None:
    """web_ui: {enabled: true} without bind/password must still fail validation."""
    import yaml
    from mailbot_v26.config_yaml import load_config, validate_config

    cfg = {
        "schema_version": 1,
        "telegram": {"bot_token": "123:tok", "chat_id": "12345"},
        "llm": {
            "provider": "cloudflare",
            "cloudflare": {"api_token": "tok", "account_id": "acc"},
            "gigachat": {"api_token": ""},
        },
        "accounts": [
            {
                "name": "Test",
                "email": "x@y.com",
                "imap_host": "imap.y.com",
                "imap_port": 993,
                "username": "x@y.com",
                "password": "pass",
                "enabled": True,
            }
        ],
        "polling": {"interval_seconds": 60, "reload_config_seconds": 300},
        "web_ui": {"enabled": True},  # Missing bind/port/password — must fail
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(cfg), encoding="utf-8")

    raw = load_config(config_path)
    ok, err = validate_config(raw)

    assert ok is False
    assert err is not None
