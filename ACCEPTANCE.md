# MailBot Premium v1 RC — Acceptance Checklist

- `python -m mailbot_v26 --version` prints a version and exits with 0.
- `python -m mailbot_v26 doctor` completes without errors.
- `python -m mailbot_v26 validate-config` returns OK with real config values.
- `letterbot.bat` creates templates on first run and stops with a warning.
- `update_and_run.bat` refuses to start polling if doctor or validation fails.
- `open_config_folder.bat` opens `mailbot_v26\config` in Explorer.
- `run_acceptance.bat` reports `ACCEPTANCE OK` on a healthy system.
- Daily digest schedule loads from `config.ini` and triggers in the expected hour.
- Weekly digest schedule loads from `config.ini` and triggers on the expected weekday/hour.
- Telegram send pipeline completes without schema deviations.
- Backup and restore flows run end-to-end with `backup.bat` and `restore.bat`.
- `update_and_run.bat` updates dependencies and launches without manual steps after config is valid.
