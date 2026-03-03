# Smoke tests (Windows)

## Scenario 1 — First run bootstrap from INI examples
- Preconditions: `settings.ini` and/or `accounts.ini` missing; both `*.example` files exist in `mailbot_v26/config`.
- Action: Run `letterbot.bat` (source) or `run_dist.bat` (dist).
- Expected: Launcher creates missing INI files, runs `config-ready`, opens `accounts.ini` when required fields are missing.

## Scenario 2 — Config readiness gate
- Preconditions: `accounts.ini` exists but required fields are blank.
- Action: Run launcher.
- Expected: clear message with required keys `login,password,host,port,use_ssl`; retry loop capped at 20.

## Scenario 3 — Dist contract check
- Action: `python -m mailbot_v26.tools.verify_dist dist/Letterbot`.
- Expected: `VERIFY_DIST PASS`.

## Scenario 4 — SmartScreen first launch
- Preconditions: unsigned `Letterbot.exe` on Windows.
- Action: Double-click `Letterbot.exe`.
- Expected: user can proceed via `More info` → `Run anyway`.
