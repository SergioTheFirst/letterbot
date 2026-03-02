from __future__ import annotations

import configparser
from pathlib import Path

from mailbot_v26.config_loader import ACCOUNT_ID_PATTERN, CONFIG_DIR
from mailbot_v26.config.ini_utils import read_user_ini_with_defaults
from mailbot_v26.config.paths import resolve_config_paths
from mailbot_v26.config_yaml import (
    SUPPORTED_SCHEMA_VERSION,
    ConfigError as YamlConfigError,
    load_config as load_yaml_config,
    validate_config_with_hints as validate_yaml_config_with_hints,
    get_schema_version,
)


SYSTEM_SECTIONS = {"telegram", "cloudflare", "gigachat", "llm"}


SETTINGS_TEMPLATE = """[general]
check_interval = 120
max_email_mb = 15
max_attachment_mb = 15
max_zip_uncompressed_mb = 80
max_extracted_chars = 50000
max_extracted_total_chars = 120000
admin_chat_id =
attention_cost_per_hour = 0

[maintenance]
maintenance_mode = 0

[storage]
db_path = data/mailbot.sqlite

[web]
host = 127.0.0.1
port = 8787

[llm]
primary = cloudflare
fallback = cloudflare

[gigachat]
enabled = true
api_key = CHANGE_ME

[cloudflare]
enabled = true

[llm_safety]
gigachat_max_consecutive_errors = 3
gigachat_max_latency_sec = 10
gigachat_cooldown_sec = 600

[ui]
locale = ru

[features]
enable_auto_priority = false
enable_daily_digest = true
enable_weekly_digest = false
enable_weekly_accuracy_report = false
enable_weekly_calibration_report = false
enable_digest_insights = false
enable_digest_action_templates = false
enable_anomaly_alerts = false
enable_attention_economics = false
enable_hierarchical_mail_types = false
enable_quality_metrics = false
enable_priority_v2 = true
enable_narrative_binding = true
enable_narrative_patterns = true
enable_circadian_delivery = true
enable_flow_protection = false
enable_attention_debt = true
enable_surprise_budget = true
enable_silence_as_signal = enabled
enable_deadlock_detection = enabled
enable_premium_processor = true
enable_premium_clarity_v1 = true
enable_behavior_metrics_digest = false
enable_trust_bootstrap = false
enable_regret_minimization = false
enable_uncertainty_queue = false
enable_commitment_chain_digest = false

[support]
; Master switch for Web UI support page: false disables /support + nav link + cockpit card.
enabled = false
; Visibility switch for nav link and cockpit card (when enabled=true, /support may still work).
show_in_nav = true
; Primary label shown on support page/cards.
label = Поддержать Letterbot
; Short helper text shown on support page.
text = Поддержать проект можно по ссылке или QR-коду
; Optional support URL (if empty, details/QR can still be shown).
url = CHANGE_ME
; Optional details line (for example: Boosty / СБП / личная страница).
details = Например: Boosty / СБП / личная страница
; Optional relative path to PNG/SVG near config dir.
qr_image = CHANGE_ME
; Weekly Telegram support footer switch (existing behavior, rate-limited).
telegram = true
; Min days between asks (hard cap: 30 by default).
min_days_between_asks = 30
; Short message shown in weekly digest footer.
message = Поддержать Letterbot → {url}

[premium_clarity]
premium_clarity_confidence_dots = auto
premium_clarity_confidence_threshold = 75
premium_clarity_confidence_dots_scale = 10

[daily_digest]
enabled = true
hour = 9
minute = 0

[weekly_digest]
weekday = mon
hour = 9
minute = 0

[weekly_accuracy_report]
window_days = 7

[weekly_calibration_report]
window_days = 7
top_n = 3
min_corrections = 10

[digest_insights]
window_days = 7
max_items = 3

[commitment_chain_digest]
window_days = 30
max_entities = 3
max_items_per_entity = 2

[behavior_metrics_digest]
window_days = 7

[uncertainty_queue]
window_days = 1
min_confidence = 70
max_items = 5

[trust_bootstrap]
learning_days = 14
min_samples = 10
max_allowed_surprise_rate = 0.30
hide_action_templates_until_ready = true
templates_window_days = 7
templates_min_corrections = 10
templates_max_surprise_rate = 0.15

[regret_minimization]
window_days = 90
trust_drop_window_days = 7
min_samples = 5

[delivery_policy]
immediate_value_threshold = 60
critical_risk_threshold = 80

[flow_protection]
focus_hours = 9-12

[auto_priority_gate]
enabled = false
window_days = 30
min_samples = 30
max_correction_rate = 0.15
cooldown_hours = 168

[deadlock_policy]
window_days = 5
min_messages = 10
cooldown_hours = 168
max_per_run = 20

[silence_policy]
lookback_days = 60
min_messages = 5
silence_factor = 3.0
min_silence_days = 7
cooldown_hours = 336
max_per_run = 20

[trust]
half_life_days = 90
weight_commitment = 0.5
weight_response = 0.3
weight_trend = 0.2
max_response_stddev_hours = 72
min_response_samples = 2
min_trend_samples = 2
"""

ACCOUNTS_TEMPLATE = """[example_account]
; account_id rules: lowercase, [a-z0-9_], no spaces.
; For Windows login use domain\\user without quotes (example: HQ\\MedvedevSS).
login = user@example.com
password = CHANGE_ME
host = imap.example.com
port = 993
use_ssl = true
telegram_chat_id = CHANGE_ME

[telegram]
bot_token = CHANGE_ME
chat_id = CHANGE_ME

[cloudflare]
account_id = CHANGE_ME
api_token = CHANGE_ME

[gigachat]
api_key = CHANGE_ME

[llm]
primary = cloudflare
fallback = cloudflare
"""


def _write_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


def _write_example(path: Path, content: str) -> bool:
    example_path = path.with_name(f"{path.name}.example")
    if example_path.exists():
        return False
    example_path.write_text(content, encoding="utf-8")
    return True


def init_config(base_dir: Path = CONFIG_DIR) -> dict[str, list[Path]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []
    examples: list[Path] = []

    targets = {
        base_dir / "settings.ini": SETTINGS_TEMPLATE,
        base_dir / "accounts.ini": ACCOUNTS_TEMPLATE,
    }

    for path, content in targets.items():
        if _write_if_missing(path, content):
            created.append(path)
            continue
        if _write_example(path, content):
            examples.append(path.with_name(f"{path.name}.example"))

    return {"created": created, "examples": examples}


def run_init_config(base_dir: Path = CONFIG_DIR) -> None:
    result = init_config(base_dir)
    created = result["created"]
    examples = result["examples"]

    print("init-config: configuration templates ready.")
    for path in created:
        print(f"CREATED: {path}")
    for path in examples:
        print(f"EXAMPLE: {path}")



def migrate_two_file_config(base_dir: Path = CONFIG_DIR) -> dict[str, list[Path]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    backups: list[Path] = []
    created: list[Path] = []

    settings_path = base_dir / "settings.ini"
    accounts_path = base_dir / "accounts.ini"
    legacy_settings = base_dir / "config.ini"
    legacy_keys = base_dir / "keys.ini"
    legacy_yaml = base_dir / "config.yaml"

    if not settings_path.exists():
        if legacy_settings.exists():
            settings_path.write_text(legacy_settings.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            settings_path.write_text(SETTINGS_TEMPLATE, encoding="utf-8")
        created.append(settings_path)

    if not accounts_path.exists():
        accounts_path.write_text(ACCOUNTS_TEMPLATE, encoding="utf-8")
        created.append(accounts_path)

    if legacy_settings.exists():
        backup = legacy_settings.with_suffix(".ini.bak")
        if not backup.exists():
            backup.write_text(legacy_settings.read_text(encoding="utf-8"), encoding="utf-8")
            backups.append(backup)


    if legacy_yaml.exists():
        backup = legacy_yaml.with_suffix(".yaml.bak")
        if not backup.exists():
            backup.write_text(legacy_yaml.read_text(encoding="utf-8"), encoding="utf-8")
            backups.append(backup)

    if legacy_keys.exists():
        parser = configparser.ConfigParser()
        parser.read(accounts_path, encoding="utf-8")
        legacy = configparser.ConfigParser()
        legacy.read(legacy_keys, encoding="utf-8")
        for section in ("telegram", "cloudflare"):
            if section in legacy and section not in parser:
                parser[section] = dict(legacy[section])
        with accounts_path.open("w", encoding="utf-8") as fh:
            parser.write(fh)
        backup = legacy_keys.with_suffix(".ini.bak")
        if not backup.exists():
            backup.write_text(legacy_keys.read_text(encoding="utf-8"), encoding="utf-8")
            backups.append(backup)

    return {"created": created, "backups": backups}


def run_migrate_config(base_dir: Path = CONFIG_DIR) -> None:
    result = migrate_two_file_config(base_dir)
    print("Using new 2-file config mode")
    for path in result["created"]:
        print(f"CREATED: {path}")
    for path in result["backups"]:
        print(f"BACKUP: {path}")


def _is_placeholder(value: str) -> bool:
    token = value.strip()
    return not token or token == "CHANGE_ME"


def _is_imap_section(section_name: str) -> bool:
    return section_name.lower() not in SYSTEM_SECTIONS


def check_config_ready(base_dir: Path = CONFIG_DIR) -> tuple[bool, list[str], list[str]]:
    critical: list[str] = []
    warnings: list[str] = []

    accounts_path = base_dir / "accounts.ini"
    if not accounts_path.exists():
        critical.append("Missing accounts.ini")
        return False, critical, warnings

    parser = read_user_ini_with_defaults(
        accounts_path,
        scope_label="config-ready accounts.ini",
    )
    if not parser.sections():
        critical.append("accounts.ini has no sections")
        return False, critical, warnings

    has_valid_imap_account = False
    for section_name in parser.sections():
        if not _is_imap_section(section_name):
            continue

        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            critical.append(
                f"Invalid IMAP account section '{section_name}': use lowercase [a-z0-9_], no spaces"
            )
            continue

        section = parser[section_name]
        missing = [
            key
            for key in ("login", "password", "host", "port", "use_ssl")
            if _is_placeholder(section.get(key, ""))
        ]
        if missing:
            critical.append(f"[{section_name}] missing required fields: {', '.join(missing)}")
            continue

        try:
            port = section.getint("port")
            if not (1 <= port <= 65535):
                critical.append(f"[{section_name}] port must be between 1 and 65535")
                continue
        except ValueError:
            critical.append(f"[{section_name}] port must be an integer")
            continue

        try:
            section.getboolean("use_ssl")
        except ValueError:
            critical.append(f"[{section_name}] use_ssl must be true/false")
            continue

        has_valid_imap_account = True

    if not has_valid_imap_account:
        critical.append("No ready IMAP account found in accounts.ini")

    telegram_section = parser["telegram"] if parser.has_section("telegram") else None
    if telegram_section is None or _is_placeholder(telegram_section.get("bot_token", "")):
        warnings.append("[telegram] bot_token is not configured (Telegram delivery may be disabled)")

    return not critical, critical, warnings


def run_config_ready(base_dir: Path = CONFIG_DIR, *, verbose: bool = False) -> int:
    ready, critical, warnings = check_config_ready(base_dir)
    if verbose:
        print("config-ready: readiness report")
        print(f"STATUS: {'OK' if ready else 'NOT_READY'}")
        for item in critical:
            print(f"CRITICAL: {item}")
        for item in warnings:
            print(f"WARN: {item}")
    return 0 if ready else 2

def validate_config(base_dir: Path = CONFIG_DIR) -> tuple[bool, list[str]]:
    issues: list[str] = []

    settings_path = base_dir / "settings.ini"
    legacy_settings_path = base_dir / "config.ini"
    accounts_path = base_dir / "accounts.ini"

    if not base_dir.exists():
        issues.append(f"Config directory not found: {base_dir}")
        return False, issues

    if not settings_path.exists() and not legacy_settings_path.exists():
        issues.append("Missing settings.ini (or legacy config.ini)")
    if not accounts_path.exists():
        issues.append("Missing accounts.ini")

    if not accounts_path.exists():
        return False, issues

    parser = read_user_ini_with_defaults(
        accounts_path,
        scope_label="validate-config accounts.ini",
    )

    if not parser.sections():
        issues.append("accounts.ini has no account sections")
        return False, issues

    has_imap_section = False

    for section_name in parser.sections():
        section = parser[section_name]
        lowered = section_name.lower()

        if lowered == "telegram":
            if _is_placeholder(section.get("bot_token", "")):
                issues.append("[telegram] bot_token is not configured")
            continue

        if lowered == "cloudflare":
            if _is_placeholder(section.get("account_id", "")):
                issues.append("[cloudflare] account_id is not configured")
            if _is_placeholder(section.get("api_token", "")):
                issues.append("[cloudflare] api_token is not configured")
            continue

        if lowered == "gigachat":
            if _is_placeholder(section.get("api_key", "")):
                issues.append("[gigachat] api_key is not configured")
            continue

        if lowered == "llm":
            if _is_placeholder(section.get("primary", "")):
                issues.append("[llm] primary is not configured")
            if _is_placeholder(section.get("fallback", "")):
                issues.append("[llm] fallback is not configured")
            continue

        has_imap_section = True
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            issues.append(
                f"Invalid account_id '{section_name}': use lowercase [a-z0-9_], no spaces"
            )
            continue

        host = section.get("host", "").strip()
        if not host:
            issues.append(f"[{section_name}] host is required")

        login = section.get("login", "").strip()
        if not login:
            issues.append(f"[{section_name}] login is required")

        password = section.get("password", "").strip()
        if not password:
            issues.append(f"[{section_name}] password is required")

        if "port" not in section:
            issues.append(f"[{section_name}] port is required")
        else:
            try:
                port = section.getint("port")
                if not (1 <= port <= 65535):
                    issues.append(f"[{section_name}] port must be between 1 and 65535")
            except ValueError:
                issues.append(f"[{section_name}] port must be an integer")

        if "use_ssl" not in section:
            issues.append(f"[{section_name}] use_ssl is required (true/false)")
        else:
            try:
                section.getboolean("use_ssl")
            except ValueError:
                issues.append(f"[{section_name}] use_ssl must be true/false")

        telegram_chat_id = section.get("telegram_chat_id", "").strip()
        if not telegram_chat_id:
            issues.append(f"[{section_name}] telegram_chat_id is recommended for Telegram delivery")

    if not has_imap_section:
        issues.append("accounts.ini has no IMAP account sections")

    return not issues, issues


def _resolve_yaml_config_path(base_dir: Path = CONFIG_DIR) -> Path | None:
    return resolve_config_paths(base_dir).yaml_path


def run_validate_config(base_dir: Path = CONFIG_DIR, *, compat: bool = False, strict: bool = False) -> int:
    if not compat:
        ok, errors = validate_config(base_dir)
        print("validate-config: configuration report")
        if ok:
            print("STATUS: OK")
            return 0

        print("STATUS: WARN")
        for error in errors:
            print(f"WARN: {error}")
        return 1 if strict else 0

    config_path = _resolve_yaml_config_path(base_dir)
    if config_path is None:
        config_path = Path(base_dir) / "config.yaml"
    try:
        raw_config = load_yaml_config(config_path)
    except (FileNotFoundError, YamlConfigError, OSError) as exc:
        print(f"[INFO] {exc}")
        return 0

    schema_version = get_schema_version(raw_config)
    ok, error, hints = validate_yaml_config_with_hints(raw_config)

    print(f"Supported schema_version: {SUPPORTED_SCHEMA_VERSION}")
    print(f"Config schema_version: {schema_version}")
    print(f"Status: {'OK' if ok else 'FAIL'}")
    if error:
        print(f"Error: {error}")
    for hint in hints:
        print(f"Hint: {hint}")

    if ok:
        return 0
    return 2
