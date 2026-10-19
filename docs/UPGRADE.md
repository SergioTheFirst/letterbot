# UPGRADE (Windows desktop installer)

## Recommended method
1. Download the new `Letterbot-Setup.exe`.
2. Close Letterbot (the installer refuses to run while `Letterbot.exe` is running).
   If you previously used the ZIP build, stop that copy too: two running copies
   notify every email twice.
3. Run the installer and let it refresh `%LOCALAPPDATA%\Letterbot`.
   Your `settings.ini`, `accounts.ini` and database are not overwritten.
4. Copy only user config files from the old install if needed:
   - `mailbot_v26/config/settings.ini`
   - `mailbot_v26/config/accounts.ini`
5. Run `%LOCALAPPDATA%\Letterbot\run.bat` and confirm startup checks pass.

Alternative: unpack `Letterbot.zip` into a new folder and copy the two config files.

The installer updates files in place and never deletes anything, so files that a
newer release no longer ships stay in the folder. For a clean reinstall, back up
`mailbot_v26\config\` and the `data\` folder, delete `%LOCALAPPDATA%\Letterbot`,
run the installer, and copy the backup back.

## Common mistakes
1. Do not copy random files over the installed folder.
2. Do not edit `Letterbot.exe`.
3. Do not edit `manifest.sha256.json`.
4. Do not browse to `http://0.0.0.0:<port>/`.

## Recovery if config is broken
1. Rename broken `settings.ini` / `accounts.ini` to `.bak`.
2. Recreate them from `settings.ini.example` / `accounts.ini.example`.
3. Re-enter values and run:
   - `python -m mailbot_v26 validate-config --config-dir mailbot_v26/config`
