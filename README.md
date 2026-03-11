# LetterBot.ru

LetterBot.ru is a self-hosted email triage assistant for long-running personal or small-team setups.
It polls IMAP mailboxes, extracts facts from messages and attachments, sends short Telegram updates, and keeps a local SQLite history for audit, replay, and the web cockpit.

## Why use it

- Designed for low-resource machines and long unattended runs
- Deterministic extraction pipeline with regression coverage
- Local-first storage and offline evaluation tooling
- Web cockpit for archive, health, and operational status
- Golden corpus gates for dangerous invoice/payroll/reconciliation regressions

## Requirements

- Python 3.10+
- Windows 10/11 or another OS with Python and SQLite available
- IMAP mailbox credentials
- Telegram bot token and destination chat ID

## Installation

1. Create and activate a virtual environment.
2. Install dependencies:
   - Windows PowerShell: `python -m pip install -r requirements.txt`
3. Initialize config templates in the repo config directory:
   - `python -m mailbot_v26 init-config --config-dir mailbot_v26/config`
4. Review and fill the generated local files:
   - `mailbot_v26/config/settings.ini`
   - `mailbot_v26/config/accounts.ini`

Two-file mode is the default. `keys.ini.example` is kept only for legacy compatibility and reference.

## Configuration

Start from the shipped templates only:

- `mailbot_v26/config/settings.ini.example`
- `mailbot_v26/config/accounts.ini.example`
- `mailbot_v26/config/keys.ini.example`

Use placeholder values such as `CHANGE_ME` as a checklist. Local `settings.ini`, `accounts.ini`, and `keys.ini` are intentionally gitignored and must never be committed.

Before first start, run:

- `python -m mailbot_v26 config-ready --config-dir mailbot_v26/config --verbose`
- `python -m mailbot_v26 validate-config --config-dir mailbot_v26/config`

These checks explain what is missing without dumping secrets or a raw traceback.

## Running LetterBot.ru

- Windows launcher: `letterbot.bat`
- Windows dist helper in the source repo: `run_dist.bat`
- Extracted Windows ZIP launcher: `run.bat`
- Direct start: `python -m mailbot_v26 --config-dir mailbot_v26/config`
- Doctor mode: `python -m mailbot_v26 doctor --config-dir mailbot_v26/config`

On successful startup, LetterBot.ru prints a short masked summary with the config directory, database path, and log path.

## Tests and quality gates

Run the core verification commands before changing behavior:

- `python -m compileall mailbot_v26 -q`
- `python -m pytest mailbot_v26/tests/ -q --tb=short`
- `python -m mailbot_v26.tools.eval_golden_corpus`

Optional maintenance status:

- `python -m mailbot_v26.tools.cleanup --status`

## Privacy and runtime notes

- Runtime state lives in local SQLite databases and log files.
- Cleanup is dry-run-first and only targets allowlisted low-value maintenance noise.
- Canonical interpretation events, feedback, and business semantics are protected from cleanup.
- Public source bundles do not include local databases, logs, caches, or local INI files.

## License

LetterBot.ru is licensed under AGPL-3.0-only. See [LICENSE](LICENSE).
