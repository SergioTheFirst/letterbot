# UPGRADE (Windows one-folder ZIP)

## Recommended method
1. Download the new `Letterbot.zip`.
2. Unzip into a NEW folder.
3. Copy only user config files from old folder:
   - `mailbot_v26/config/settings.ini`
   - `mailbot_v26/config/accounts.ini`
4. Run `run_dist.bat` and confirm startup checks pass.

## Common mistakes
1. Do not unzip over old folder.
2. Do not edit `Letterbot.exe`.
3. Do not edit `manifest.sha256.json`.
4. Do not browse to `http://0.0.0.0:<port>/`.

## Recovery if config is broken
1. Rename broken `settings.ini` / `accounts.ini` to `.bak`.
2. Recreate from `settings.ini.example` / `accounts.ini.example`.
3. Re-enter values and run:
   - `python -m mailbot_v26 validate-config --config-dir mailbot_v26/config`
