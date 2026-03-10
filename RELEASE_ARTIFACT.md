# Release Artifact Contract

This repository ships a Windows one-folder artifact built by
`build_windows_onefolder.bat` and verified by `verify_dist.bat`.

## Source-repo helpers

- `letterbot.bat` - source-mode launcher
- `run_dist.bat` - repository helper for local dist smoke checks
- `build_windows_onefolder.bat` - one-folder builder
- `verify_dist.bat` - manifest and contract verifier

## Required files in `dist/Letterbot`

- `Letterbot.exe`
- `run.bat`
- `mailbot_v26/config/settings.ini.example`
- `mailbot_v26/config/accounts.ini.example`
- `README_QUICKSTART_WINDOWS.md`
- `manifest.sha256.json`
- `UPGRADE.md`
- `SMARTSCREEN.md`
- `CHANGELOG.md`

`run.bat` is the extracted release launcher. It is produced from the repository
helper `run_dist.bat` during the build.

## First run from the ZIP artifact

1. Extract `Letterbot.zip` to a new folder.
2. Open the extracted `Letterbot` folder.
3. Run `run.bat`.
4. Fill `mailbot_v26/config/settings.ini` and `mailbot_v26/config/accounts.ini`.
5. Run `run.bat` again.

## Local release verification

1. Create `.venv` and install `requirements.txt` plus `requirements-build.txt`.
2. Run `build_windows_onefolder.bat`.
3. Run `verify_dist.bat`.
4. Package `dist/Letterbot` only after verification passes.
