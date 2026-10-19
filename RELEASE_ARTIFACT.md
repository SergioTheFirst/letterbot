# Release Artifact Contract

This repository ships a Windows desktop bundle built by
`build_windows_onefolder.bat`.

Public release artifacts:

- `dist/Letterbot.zip` (one-folder bundle, always produced by CI)
- `dist/Letterbot-Setup.exe` (single-file installer; CI smoke-installs it and
  verifies the installed tree; locally set `LETTERBOT_BUILD_INSTALLER=1`)

Canonical internal build basis:

- `dist/Letterbot/` verified by `verify_dist.bat`

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

## First run from the installer

1. Run `Letterbot-Setup.exe`.
2. Wait until it installs LetterBot.ru into `%LOCALAPPDATA%\Letterbot`.
3. The installer launches `run.bat` from the installed folder.
4. Fill `mailbot_v26/config/settings.ini` and `mailbot_v26/config/accounts.ini`.
5. Run `%LOCALAPPDATA%\Letterbot\run.bat` again if needed.

## Local release verification

1. Create `.venv` and install `requirements.txt` plus `requirements-build.txt`.
2. Run `build_windows_onefolder.bat`.
3. Run `verify_dist.bat`.
4. To also build the installer, set `LETTERBOT_BUILD_INSTALLER=1` before step 2
   and confirm `dist/Letterbot-Setup.exe` exists after the build.
