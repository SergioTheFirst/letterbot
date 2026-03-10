# Letterbot Premium - Windows Quickstart

## 1) Installation and first start
1. Download or clone the repository.
2. Open the project folder.
3. Run `letterbot.bat`.

The launcher creates `.venv`, installs dependencies, and starts `python -m mailbot_v26`.

## 2) Where the config lives
All config files live in `mailbot_v26/config/`:
- `settings.ini` - general settings (web, storage, feature flags)
- `accounts.ini` - IMAP accounts and Telegram chat settings

## 3) Diagnostics: doctor mode
If the bot does not start, run:

```powershell
python -m mailbot_v26 doctor --config-dir mailbot_v26/config
```

## 4) Useful commands
- Source mode: `letterbot.bat`
- Config readiness: `python -m mailbot_v26 config-ready --config-dir mailbot_v26/config --verbose`
- Validation: `python -m mailbot_v26 validate-config --config-dir mailbot_v26/config`
- Extracted ZIP dist mode: `run.bat`
- Repository-side dist helper: `run_dist.bat`
- Test suite: `run_tests.bat`
