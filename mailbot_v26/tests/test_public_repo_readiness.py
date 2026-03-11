from __future__ import annotations

import configparser
import re
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "mailbot_v26" / "config"
TELEGRAM_TOKEN_RE = re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{20,}\b")
REAL_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@(?!example\.com\b)[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_settings_example_has_no_real_credentials() -> None:
    parser = configparser.ConfigParser()
    parser.read(CONFIG_DIR / "settings.ini.example", encoding="utf-8")

    assert parser.get("web_ui", "password") == "CHANGE_ME"
    assert parser.get("gigachat", "api_key") == "CHANGE_ME"
    assert TELEGRAM_TOKEN_RE.search(_read(CONFIG_DIR / "settings.ini.example")) is None


def test_keys_example_has_only_placeholders() -> None:
    parser = configparser.ConfigParser()
    parser.read(CONFIG_DIR / "keys.ini.example", encoding="utf-8")

    assert parser.get("telegram", "bot_token") == "CHANGE_ME"
    assert parser.get("cloudflare", "account_id") == "CHANGE_ME"
    assert parser.get("cloudflare", "api_token") == "CHANGE_ME"


def test_accounts_example_has_only_placeholders_or_safe_examples() -> None:
    parser = configparser.ConfigParser()
    parser.read(CONFIG_DIR / "accounts.ini.example", encoding="utf-8")

    assert parser.get("example_account", "login") == "user@example.com"
    assert parser.get("example_account", "host") == "imap.example.com"
    assert parser.get("example_account", "password") == "CHANGE_ME"
    assert parser.get("telegram", "bot_token") == "CHANGE_ME"
    assert parser.get("gigachat", "api_key") == "CHANGE_ME"


def test_no_hardcoded_credentials_in_python_files() -> None:
    offenders: list[str] = []
    for path in (REPO_ROOT / "mailbot_v26").rglob("*.py"):
        relative = path.relative_to(REPO_ROOT).as_posix()
        if "/tests/" in f"/{relative}":
            continue
        text = _read(path)
        if TELEGRAM_TOKEN_RE.search(text):
            offenders.append(relative)
            continue
        if REAL_EMAIL_RE.search(text):
            offenders.append(relative)
    assert offenders == []


def test_local_real_config_files_are_gitignored() -> None:
    git = shutil.which("git")
    assert git is not None

    result = subprocess.run(
        [git, "check-ignore", "settings.ini", "accounts.ini", "keys.ini", ".env"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    ignored = {line.strip() for line in result.splitlines() if line.strip()}

    assert {"settings.ini", "accounts.ini", "keys.ini", ".env"}.issubset(ignored)


def test_readme_exists() -> None:
    assert (REPO_ROOT / "README.md").exists()


def test_readme_has_installation_section() -> None:
    text = _read(REPO_ROOT / "README.md")

    assert "## Installation" in text
    assert "## Configuration" in text
    assert "## Running LetterBot.ru" in text


def test_readme_has_no_real_credentials() -> None:
    text = _read(REPO_ROOT / "README.md")

    assert TELEGRAM_TOKEN_RE.search(text) is None
    assert REAL_EMAIL_RE.search(text) is None


def test_license_exists() -> None:
    assert (REPO_ROOT / "LICENSE").exists()


def test_license_is_agpl3_or_project_expected_license() -> None:
    text = _read(REPO_ROOT / "LICENSE")

    assert "GNU AFFERO GENERAL PUBLIC LICENSE" in text
    assert "Version 3, 19 November 2007" in text


def test_contributing_exists() -> None:
    assert (REPO_ROOT / "CONTRIBUTING.md").exists()
