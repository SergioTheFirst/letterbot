from __future__ import annotations

from pathlib import Path
import ipaddress
from dataclasses import dataclass
from typing import Any, Tuple

from mailbot_v26 import deps

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    IngestConfig,
    KeysConfig,
    MaintenanceConfig,
    StorageConfig,
)

DEFAULT_CHECK_INTERVAL_SEC = 60
DEFAULT_RELOAD_CONFIG_SEC = 60
DEFAULT_MAX_EMAIL_MB = 15
DEFAULT_MAX_ATTACHMENT_MB = 15
DEFAULT_MAX_ZIP_UNCOMPRESSED_MB = 80
DEFAULT_MAX_EXTRACTED_CHARS = 50_000
DEFAULT_MAX_EXTRACTED_TOTAL_CHARS = 120_000
SUPPORTED_SCHEMA_VERSION = 1

SCHEMA_NEWER_MESSAGE = (
    "Конфиг новее этой версии MailBot. Скачайте более новую версию или "
    "используйте config.example.yaml этой версии. См. docs/UPGRADE.md"
)

SCHEMA_OLDER_HINT = (
    "Конфиг старой схемы. Запустите: python -m mailbot_v26 validate-config --compat"
)


class ConfigError(RuntimeError):
    def __init__(self, message: str, *, raw_detail: str | None = None) -> None:
        super().__init__(message)
        self.raw_detail = raw_detail


WINDOWS_BACKSLASH_HINT = (
    "Use single quotes for Windows usernames/paths, e.g. 'HQ\\User', or "
    "escape backslashes as \\\\" 
)


def _is_windows_backslash_yaml_error(detail: str) -> bool:
    lowered = detail.lower()
    has_escape_error = "unknown escape" in lowered or "invalid escape" in lowered
    return has_escape_error and "\\" in detail


def format_yaml_parse_error_message(detail: str) -> str:
    if _is_windows_backslash_yaml_error(detail):
        return f"Failed to parse config.yaml. {WINDOWS_BACKSLASH_HINT}"
    return "Failed to parse config.yaml. Please fix YAML syntax in config.yaml."


@dataclass(frozen=True)
class ConfigFeatures:
    donate_enabled: bool = False


def resolve_support_enabled(cfg: dict[str, Any]) -> bool:
    support_raw = cfg.get("support") if isinstance(cfg, dict) else None
    if isinstance(support_raw, dict) and "enabled" in support_raw:
        return bool(support_raw.get("enabled", False))
    features_raw = cfg.get("features") if isinstance(cfg, dict) else None
    if isinstance(features_raw, dict):
        return bool(features_raw.get("donate_enabled", False))
    return False


def _load_features_config(cfg: dict[str, Any]) -> ConfigFeatures:
    features_raw = cfg.get("features")
    if features_raw is None:
        return ConfigFeatures()
    if not isinstance(features_raw, dict):
        raise ValueError("Ошибка в config.yaml: features должен быть словарём")
    donate_enabled = features_raw.get("donate_enabled", False)
    if not isinstance(donate_enabled, bool):
        raise ValueError("Ошибка в config.yaml: features.donate_enabled должен быть true/false")
    return ConfigFeatures(donate_enabled=resolve_support_enabled(cfg))


def _yaml_module():
    deps.require("yaml", "PyYAML", "Нужен для загрузки config.yaml")
    import yaml  # type: ignore

    return yaml


def load_config(path: str | Path = "config.yaml") -> dict[str, Any]:
    yaml = _yaml_module()
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml not found: {config_path.resolve()}")
    try:
        content = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Failed to read config.yaml: {exc}") from exc
    try:
        payload = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raw_detail = str(exc)
        raise ConfigError(
            format_yaml_parse_error_message(raw_detail),
            raw_detail=raw_detail,
        ) from exc
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ConfigError("config.yaml must contain a mapping at the root")
    return payload


def validate_config(cfg: dict[str, Any]) -> Tuple[bool, str]:
    try:
        ok, error, _hint = validate_schema_version(cfg)
        if not ok:
            return False, error or "Ошибка в config.yaml: schema_version некорректен"

        if not isinstance(cfg, dict):
            return False, "Ошибка в config.yaml: корневой объект должен быть словарём"

        accounts = cfg.get("accounts")
        if not isinstance(accounts, list):
            return False, "Ошибка в config.yaml: accounts отсутствует"
        if not accounts:
            return False, "Ошибка в config.yaml: accounts пустой"

        top_level_imap = cfg.get("imap")
        for index, account in enumerate(accounts):
            if not isinstance(account, dict):
                return False, f"Ошибка в config.yaml: accounts[{index}] должен быть словарём"
            if not _is_non_empty_str(account.get("name")):
                return False, f"Ошибка в config.yaml: accounts[{index}].name отсутствует"
            if not _is_non_empty_str(account.get("email")):
                return False, f"Ошибка в config.yaml: accounts[{index}].email отсутствует"

            account_imap = account.get("imap")
            if account_imap is not None and not isinstance(account_imap, dict):
                return False, f"Ошибка в config.yaml: accounts[{index}].imap должен быть словарём"

            imap_host = account.get("imap_host")
            if isinstance(top_level_imap, dict):
                imap_host = top_level_imap.get("host", imap_host)
            if isinstance(account_imap, dict):
                imap_host = account_imap.get("host", imap_host)
            if not _is_non_empty_str(imap_host):
                return False, f"Ошибка в config.yaml: accounts[{index}].imap_host отсутствует"

            imap_port = account.get("imap_port")
            if isinstance(top_level_imap, dict):
                imap_port = top_level_imap.get("port", imap_port)
            if isinstance(account_imap, dict):
                imap_port = account_imap.get("port", imap_port)
            if not _is_int(imap_port):
                return False, f"Ошибка в config.yaml: accounts[{index}].imap_port отсутствует"

            if not _is_non_empty_str(account.get("username")):
                return False, f"Ошибка в config.yaml: accounts[{index}].username отсутствует"
            if not _is_non_empty_str(account.get("password")):
                return False, f"Ошибка в config.yaml: accounts[{index}].password отсутствует"
            if not isinstance(account.get("enabled"), bool):
                return False, f"Ошибка в config.yaml: accounts[{index}].enabled отсутствует"

        telegram = cfg.get("telegram")
        if not isinstance(telegram, dict):
            return False, "Ошибка в config.yaml: telegram отсутствует"
        if not _is_non_empty_str(telegram.get("bot_token")):
            return False, "Ошибка в config.yaml: telegram.bot_token отсутствует"
        if not _is_non_empty_str(telegram.get("chat_id")):
            return False, "Ошибка в config.yaml: telegram.chat_id отсутствует"

        llm = cfg.get("llm")
        if not isinstance(llm, dict):
            return False, "Ошибка в config.yaml: llm отсутствует"
        provider = llm.get("provider")
        if provider not in {"cloudflare", "gigachat"}:
            return False, 'Ошибка в config.yaml: llm.provider должен быть "cloudflare" или "gigachat"'

        cloudflare = llm.get("cloudflare")
        gigachat = llm.get("gigachat")
        if provider == "cloudflare":
            if not isinstance(cloudflare, dict):
                return False, "Ошибка в config.yaml: llm.cloudflare отсутствует"
            if not _is_non_empty_str(cloudflare.get("api_token")):
                return False, "Ошибка в config.yaml: llm.cloudflare.api_token отсутствует"
            if not _is_non_empty_str(cloudflare.get("account_id")):
                return False, "Ошибка в config.yaml: llm.cloudflare.account_id отсутствует"
        if provider == "gigachat":
            if not isinstance(gigachat, dict):
                return False, "Ошибка в config.yaml: llm.gigachat отсутствует"
            if not _is_non_empty_str(gigachat.get("api_token")):
                return False, "Ошибка в config.yaml: llm.gigachat.api_token отсутствует"

        for key, section in (("llm.cloudflare", cloudflare), ("llm.gigachat", gigachat)):
            if section is None:
                continue
            if not isinstance(section, dict):
                return False, f"Ошибка в config.yaml: {key} должен быть словарём"
            model_value = section.get("model", "MISSING")
            if model_value is not None and model_value != "MISSING" and not isinstance(model_value, str):
                return False, f"Ошибка в config.yaml: {key}.model должен быть строкой или null"

        polling = cfg.get("polling", {})
        if polling is not None and not isinstance(polling, dict):
            return False, "Ошибка в config.yaml: polling должен быть словарём"
        interval = polling.get("interval_seconds", DEFAULT_CHECK_INTERVAL_SEC)
        reload_interval = polling.get("reload_config_seconds", DEFAULT_RELOAD_CONFIG_SEC)
        if not _is_int(interval):
            return False, "Ошибка в config.yaml: polling.interval_seconds отсутствует"
        if not _is_int(reload_interval):
            return False, "Ошибка в config.yaml: polling.reload_config_seconds отсутствует"

        web_ui = cfg.get("web_ui")
        if web_ui is not None:
            if not isinstance(web_ui, dict):
                return False, "Ошибка в config.yaml: web_ui должен быть словарём"
            enabled = web_ui.get("enabled")
            if not isinstance(enabled, bool):
                return False, "Ошибка в config.yaml: web_ui.enabled должен быть true/false"
            bind = web_ui.get("bind", "127.0.0.1")
            if bind is not None and (not isinstance(bind, str) or not bind.strip()):
                return False, "Ошибка в config.yaml: web_ui.bind должен быть строкой"
            port = web_ui.get("port", 8787)
            if port is not None and not _is_port(port):
                return False, "Ошибка в config.yaml: web_ui.port должен быть числом 1..65535"
            password = web_ui.get("password", "")
            if enabled and not _is_non_empty_str(password):
                return False, "Ошибка в config.yaml: web_ui.password отсутствует"
            api_token = web_ui.get("api_token", "")
            if api_token is not None and not isinstance(api_token, str):
                return False, "Ошибка в config.yaml: web_ui.api_token должен быть строкой"
            allow_lan = web_ui.get("allow_lan", False)
            if not isinstance(allow_lan, bool):
                return False, "Ошибка в config.yaml: web_ui.allow_lan должен быть true/false"
            prod_server = web_ui.get("prod_server", False)
            if not isinstance(prod_server, bool):
                return False, "Ошибка в config.yaml: web_ui.prod_server должен быть true/false"
            require_strong_password_on_lan = web_ui.get("require_strong_password_on_lan", True)
            if not isinstance(require_strong_password_on_lan, bool):
                return False, "Ошибка в config.yaml: web_ui.require_strong_password_on_lan должен быть true/false"
            allow_cidrs = web_ui.get("allow_cidrs", [])
            if allow_cidrs is None:
                allow_cidrs = []
            if not isinstance(allow_cidrs, list):
                return False, "Ошибка в config.yaml: web_ui.allow_cidrs должен быть списком"
            for index, cidr in enumerate(allow_cidrs):
                if not _is_non_empty_str(cidr):
                    return False, f"Ошибка в config.yaml: web_ui.allow_cidrs[{index}] должен быть строкой"
                if not _is_valid_cidr(cidr):
                    return False, f"Ошибка в config.yaml: web_ui.allow_cidrs[{index}] некорректный CIDR"
            if enabled and not _is_loopback_bind(bind):
                if not allow_lan:
                    return False, "Ошибка в config.yaml: web_ui.allow_lan должен быть true для bind вне loopback"
                if not allow_cidrs:
                    return False, "Ошибка в config.yaml: web_ui.allow_cidrs должен быть непустым при allow_lan=true"
                if require_strong_password_on_lan:
                    if _is_default_weak_web_password(str(password)):
                        return False, "Ошибка в config.yaml: web_ui.password не должен быть значением по умолчанию для LAN"
                    if len(str(password)) < 10:
                        return False, "Ошибка в config.yaml: web_ui.password должен быть не короче 10 символов для LAN"

        support = cfg.get("support")
        if support is not None:
            if not isinstance(support, dict):
                return False, "Ошибка в config.yaml: support должен быть словарём"
            enabled = support.get("enabled", False)
            if not isinstance(enabled, bool):
                return False, "Ошибка в config.yaml: support.enabled должен быть true/false"

            methods = support.get("methods", [])
            if not isinstance(methods, list):
                return False, "Ошибка в config.yaml: support.methods должен быть списком"
            if enabled and len(methods) == 0:
                return False, "Ошибка в config.yaml: support.methods должен содержать хотя бы один метод"
            for index, method in enumerate(methods):
                if not isinstance(method, dict):
                    return False, f"Ошибка в config.yaml: support.methods[{index}] должен быть словарём"
                method_type = str(method.get("type", "")).strip().lower()
                if not method_type:
                    return False, f"Ошибка в config.yaml: support.methods[{index}].type отсутствует"
                if method_type == "card" and not _is_non_empty_str(method.get("number")):
                    return False, f"Ошибка в config.yaml: support.methods[{index}].number отсутствует"
                if method_type == "yoomoney":
                    url = str(method.get("url", "")).strip().lower()
                    if not url:
                        return False, f"Ошибка в config.yaml: support.methods[{index}].url отсутствует"
                    if not (url.startswith("http://") or url.startswith("https://")):
                        return False, f"Ошибка в config.yaml: support.methods[{index}].url должен начинаться с http(s)"

            telegram_support = support.get("telegram", {})
            if telegram_support is not None:
                if not isinstance(telegram_support, dict):
                    return False, "Ошибка в config.yaml: support.telegram должен быть словарём"
                tg_enabled = telegram_support.get("enabled", False)
                if not isinstance(tg_enabled, bool):
                    return False, "Ошибка в config.yaml: support.telegram.enabled должен быть true/false"
                frequency_days = telegram_support.get("frequency_days", 30)
                if not isinstance(frequency_days, int) or isinstance(frequency_days, bool):
                    return False, "Ошибка в config.yaml: support.telegram.frequency_days должен быть числом 7..365"
                if frequency_days < 7 or frequency_days > 365:
                    return False, "Ошибка в config.yaml: support.telegram.frequency_days должен быть числом 7..365"
                text = telegram_support.get("text", "")
                if tg_enabled and not _is_non_empty_str(text):
                    return False, "Ошибка в config.yaml: support.telegram.text отсутствует"

        _load_features_config(cfg)

        return True, ""
    except Exception as exc:
        if str(exc).startswith("Ошибка в config.yaml:"):
            return False, str(exc)
        return False, f"Ошибка в config.yaml: внутренняя ошибка валидации ({exc})"


def validate_config_with_hints(cfg: dict[str, Any]) -> tuple[bool, str | None, list[str]]:
    ok, error = validate_config(cfg)
    if not ok:
        return False, error, []

    schema_ok, schema_error, schema_hint = validate_schema_version(cfg)
    if not schema_ok:
        return False, schema_error, []

    hints = [schema_hint] if schema_hint else []
    return True, None, hints


def get_schema_version(cfg: dict[str, Any]) -> int:
    if not isinstance(cfg, dict):
        return 1
    value = cfg.get("schema_version", 1)
    if value is None:
        return 1
    if isinstance(value, bool):
        return -1
    if isinstance(value, int):
        return value
    return -1


def validate_schema_version(cfg: dict[str, Any]) -> tuple[bool, str | None, str | None]:
    schema_version = get_schema_version(cfg)
    if schema_version < 1:
        return False, "Ошибка в config.yaml: schema_version должен быть целым числом >= 1", None
    if schema_version > SUPPORTED_SCHEMA_VERSION:
        return False, SCHEMA_NEWER_MESSAGE, None
    if schema_version < SUPPORTED_SCHEMA_VERSION:
        return True, None, SCHEMA_OLDER_HINT
    return True, None, None


def get_polling_intervals(cfg: dict[str, Any]) -> tuple[int, int]:
    polling = cfg.get("polling", {})
    interval = polling.get("interval_seconds", DEFAULT_CHECK_INTERVAL_SEC)
    reload_interval = polling.get("reload_config_seconds", DEFAULT_RELOAD_CONFIG_SEC)
    return int(interval), int(reload_interval)


def build_bot_config(cfg: dict[str, Any], *, repo_root: Path) -> BotConfig:
    telegram = cfg.get("telegram", {})
    llm_section = cfg.get("llm", {})
    cloudflare = llm_section.get("cloudflare", {}) if isinstance(llm_section, dict) else {}
    interval, _reload = get_polling_intervals(cfg)

    general = GeneralConfig(
        check_interval=int(interval),
        max_email_mb=DEFAULT_MAX_EMAIL_MB,
        max_attachment_mb=DEFAULT_MAX_ATTACHMENT_MB,
        max_zip_uncompressed_mb=DEFAULT_MAX_ZIP_UNCOMPRESSED_MB,
        max_extracted_chars=DEFAULT_MAX_EXTRACTED_CHARS,
        max_extracted_total_chars=DEFAULT_MAX_EXTRACTED_TOTAL_CHARS,
        admin_chat_id=str(telegram.get("chat_id", "")).strip(),
    )

    telegram_chat_id = str(telegram.get("chat_id", "")).strip()
    accounts: list[AccountConfig] = []
    for account in cfg.get("accounts", []):
        enabled = bool(account.get("enabled", True))
        if not enabled:
            continue
        email = str(account.get("email", "")).strip()
        name = str(account.get("name", "")).strip()
        imap_port = int(account.get("imap_port", 993))
        use_ssl = imap_port == 993
        accounts.append(
            AccountConfig(
                account_id=email or name or "unknown",
                login=email or str(account.get("username", "")).strip(),
                username=str(account.get("username", "")).strip(),
                name=name,
                password=str(account.get("password", "")).strip(),
                host=str(account.get("imap_host", "")).strip(),
                port=imap_port,
                use_ssl=use_ssl,
                telegram_chat_id=telegram_chat_id,
                enabled=enabled,
            )
        )

    keys = KeysConfig(
        telegram_bot_token=str(telegram.get("bot_token", "")).strip(),
        cf_account_id=str(cloudflare.get("account_id", "")).strip(),
        cf_api_token=str(cloudflare.get("api_token", "")).strip(),
    )

    db_path = repo_root / "data" / "mailbot.sqlite"
    storage = StorageConfig(db_path=db_path)

    return BotConfig(
        general=general,
        accounts=accounts,
        keys=keys,
        storage=storage,
        ingest=IngestConfig(allow_prestart_emails=False),
        maintenance=MaintenanceConfig(maintenance_mode=False),
    )


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    if isinstance(value, str) and value.isdigit():
        return int(value) > 0
    return False


def _is_port(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return 1 <= value <= 65535
    if isinstance(value, str) and value.isdigit():
        return 1 <= int(value) <= 65535
    return False


def _is_loopback_bind(bind: str) -> bool:
    if not bind:
        return False
    if bind.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(bind).is_loopback
    except ValueError:
        return False


def _is_valid_cidr(cidr: str) -> bool:
    try:
        ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return False
    return True


def _is_default_weak_web_password(password: str) -> bool:
    normalized = password.strip().lower()
    blocked = {
        "change_me",
        "changeme",
        "password",
        "1234",
        "12345",
        "123456",
        "qwerty",
    }
    return normalized in blocked
