from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "mailbot_v26" / "config"
SOURCE_PATH = CONFIG_DIR / "config.ini.example"
TARGET_PATH = CONFIG_DIR / "config.ini.compact.example"


def _build_compact_template() -> str:
    sections = [
        """; Letterbot compact INI template.
; Full template with all options: mailbot_v26/config/config.ini.example

[general]
check_interval = 120
admin_chat_id =

[storage]
db_path = data/mailbot.sqlite

[llm]
primary = gigachat
fallback = cloudflare

[gigachat]
enabled = true
api_key = CHANGE_ME

[cloudflare]
enabled = true

[ui]
locale = ru

; Advanced / Rare / Debug
[maintenance]
maintenance_mode = 0

[llm_safety]
gigachat_max_consecutive_errors = 3
gigachat_max_latency_sec = 10
gigachat_cooldown_sec = 600

[llm_usage]
llm_percentile_threshold = 80
llm_usage_window_days = 7
force_llm_always = false

[features]
enable_daily_digest = true
enable_weekly_digest = true
; Support features (donate/support UI). Keep false for private/self-hosted.
support = false
; Backward-compatible key (optional):
donate_enabled = false
"""
    ]
    return "\n".join(part.strip("\n") for part in sections) + "\n"


def make_ini_compact(source_path: Path = SOURCE_PATH, target_path: Path = TARGET_PATH) -> Path:
    if not source_path.exists():
        raise FileNotFoundError(f"Missing source template: {source_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(_build_compact_template(), encoding="utf-8")
    return target_path


def main() -> None:
    written = make_ini_compact()
    print(f"Wrote compact template: {written}")


if __name__ == "__main__":
    main()
