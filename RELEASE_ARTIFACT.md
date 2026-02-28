# RELEASE_ARTIFACT.md — v28 RC artifact contract

## Required files in release package
- `requirements.txt`
- `mailbot_v26/config/accounts.ini.example`
- `mailbot_v26/config/settings.ini.example`
- `update_and_run.bat`
- `install_and_run.bat`
- `CHANGELOG.md`
- `CONTINUITY.md`
- `MANIFEST.json`

## Windows: minimal first run
1. Open terminal in project root.
2. Run `install_and_run.bat`.
3. Fill `mailbot_v26/config/accounts.ini` and `mailbot_v26/config/settings.ini` if prompted.
4. Run `update_and_run.bat` for normal update/start cycle.

## Windows: minimal repair
1. Run `python -m mailbot_v26 doctor --strict --config-dir mailbot_v26/config`.
2. Recreate virtualenv if needed: delete `.venv` then run `install_and_run.bat`.
3. Re-run `update_and_run.bat` and check `logs/update_and_run.log` on failures.

## Linux/macOS: minimal run
1. `python -m venv .venv && . .venv/bin/activate`
2. `python -m pip install -r requirements.txt`
3. `python -m mailbot_v26 migrate-config --config-dir mailbot_v26/config`
4. `python -m mailbot_v26.start --config-dir mailbot_v26/config`
