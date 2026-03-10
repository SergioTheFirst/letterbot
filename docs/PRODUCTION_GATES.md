# Production Gates (Windows one-folder ZIP)

## G1 - First-run succeeds
- User can unpack `Letterbot.zip`, run `run.bat`, fill `accounts.ini`, and reach steady state.
- First run bootstraps `settings.ini` and `accounts.ini` from `*.example`.

## G2 - Config errors are actionable
- `config-ready` and `validate-config` fail fast with explicit field-level output.
- New-install flow does not require `config.yaml`.

## G3 - LAN UI safety
- LAN bind rules and CIDR restrictions are enforced through `settings.ini`.

## G4 - Support bundle sufficient
- `/doctor/export` produces `diagnostics.zip` with redacted config and manifest status.

## G5 - Build reproducibility
- `build_windows_onefolder.bat` produces `dist/Letterbot`.
- Required contract files:
  - `Letterbot.exe`
  - `run.bat`
  - `mailbot_v26/config/settings.ini.example`
  - `mailbot_v26/config/accounts.ini.example`
  - `README_QUICKSTART_WINDOWS.md`
  - `manifest.sha256.json`
- `python -m mailbot_v26.tools.verify_dist dist/Letterbot` returns PASS.
