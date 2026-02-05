from mailbot_v26.config_yaml import validate_config


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
    }


def test_validate_config_success() -> None:
    ok, error = validate_config(_base_config())

    assert ok
    assert error is None


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
    assert error == 'Ошибка в config.yaml: llm.provider должен быть "cloudflare" или "gigachat"'


def test_validate_config_model_null_allowed() -> None:
    config = _base_config()
    config["llm"]["cloudflare"]["model"] = None

    ok, error = validate_config(config)

    assert ok
    assert error is None
