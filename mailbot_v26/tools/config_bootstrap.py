from __future__ import annotations

import configparser
from pathlib import Path

from mailbot_v26.config_loader import ACCOUNT_ID_PATTERN, CONFIG_DIR


CONFIG_TEMPLATE = """[general]
check_interval = 120
max_email_mb = 15
max_attachment_mb = 15
max_zip_uncompressed_mb = 80
max_extracted_chars = 50000
max_extracted_total_chars = 120000
admin_chat_id =

[storage]
db_path = data/mailbot.sqlite

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
enable_attention_debt = true
enable_surprise_budget = true
enable_silence_as_signal = shadow
enable_deadlock_detection = shadow
enable_premium_processor = false
enable_behavior_metrics_digest = false
enable_trust_bootstrap = false

[daily_digest]
hour = 9
minute = 0

[weekly_digest]
weekday = mon
hour = 9
minute = 0

[digest_insights]
window_days = 7
max_items = 3

[behavior_metrics_digest]
window_days = 7

[trust_bootstrap]
learning_days = 14
min_samples = 50
max_allowed_surprise_rate = 0.30
hide_action_templates_until_ready = true

[delivery_policy]
night_hours = 21-7
immediate_value_threshold = 60
batch_value_threshold = 20
critical_risk_threshold = 80
max_immediate_per_hour = 5

[auto_priority_gate]
enabled = false
window_days = 30
min_samples = 30
max_correction_rate = 0.15
cooldown_hours = 24

[deadlock_policy]
window_days = 5
min_messages = 10
cooldown_hours = 24
max_per_run = 20

[silence_policy]
lookback_days = 60
min_messages = 6
silence_factor = 3.0
min_silence_days = 7
cooldown_hours = 72
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
; Duplicate this section per mailbox, change the section name.
login = user@example.com
password = CHANGE_ME
host = imap.example.com
port = 993
use_ssl = true
telegram_chat_id = CHANGE_ME
"""

KEYS_TEMPLATE = """[telegram]
bot_token = CHANGE_ME

[cloudflare]
account_id = CHANGE_ME
api_token = CHANGE_ME
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
        base_dir / "config.ini": CONFIG_TEMPLATE,
        base_dir / "accounts.ini": ACCOUNTS_TEMPLATE,
        base_dir / "keys.ini": KEYS_TEMPLATE,
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


def validate_config(base_dir: Path = CONFIG_DIR) -> tuple[bool, list[str]]:
    errors: list[str] = []

    config_path = base_dir / "config.ini"
    accounts_path = base_dir / "accounts.ini"
    keys_path = base_dir / "keys.ini"

    if not base_dir.exists():
        errors.append(f"Config directory not found: {base_dir}")
        return False, errors

    if not config_path.exists():
        errors.append("Missing config.ini")
    if not accounts_path.exists():
        errors.append("Missing accounts.ini")
    if not keys_path.exists():
        errors.append("Missing keys.ini")

    if not accounts_path.exists():
        return False, errors

    parser = configparser.ConfigParser()
    parser.read(accounts_path, encoding="utf-8")

    if not parser.sections():
        errors.append("accounts.ini has no account sections")
        return False, errors

    for section_name in parser.sections():
        if not ACCOUNT_ID_PATTERN.fullmatch(section_name):
            errors.append(
                f"Invalid account_id '{section_name}': use lowercase [a-z0-9_], no spaces"
            )
            continue

        section = parser[section_name]

        host = section.get("host", "").strip()
        if not host:
            errors.append(f"[{section_name}] host is required")

        if "port" not in section:
            errors.append(f"[{section_name}] port is required")
        else:
            try:
                port = section.getint("port")
                if not (1 <= port <= 65535):
                    errors.append(f"[{section_name}] port must be between 1 and 65535")
            except ValueError:
                errors.append(f"[{section_name}] port must be an integer")

        if "use_ssl" not in section:
            errors.append(f"[{section_name}] use_ssl is required (true/false)")
        else:
            try:
                section.getboolean("use_ssl")
            except ValueError:
                errors.append(f"[{section_name}] use_ssl must be true/false")

        telegram_chat_id = section.get("telegram_chat_id", "").strip()
        if not telegram_chat_id:
            errors.append(f"[{section_name}] telegram_chat_id is required")

    return not errors, errors


def run_validate_config(base_dir: Path = CONFIG_DIR) -> int:
    ok, errors = validate_config(base_dir)
    print("validate-config: configuration report")
    if ok:
        print("STATUS: OK")
        return 0

    print("STATUS: FAIL")
    for error in errors:
        print(f"ERROR: {error}")
    return 1
