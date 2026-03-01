# Windows Release Checklist

## Build
1. Ensure `.venv` contains runtime + build deps.
2. Run `build_windows_onefolder.bat`.
3. Confirm output folder `dist\Letterbot`.
4. Confirm required files (per current `pyinstaller.spec` layout):
   - `Letterbot.exe`
   - `run.bat`
   - `mailbot_v26\config\settings.ini.example`
   - `mailbot_v26\config\accounts.ini.example`
   - `README_QUICKSTART_WINDOWS.md`
   - `manifest.sha256.json`
5. Run `verify_dist.bat`.

## Package
1. Create zip: `Compress-Archive dist/Letterbot dist/Letterbot.zip`.
2. Upload artifact name: `Letterbot-windows-onefolder`.

## Smoke
1. On clean machine extract `Letterbot.zip`.
2. Run `run.bat`.
3. Fill required `accounts.ini` fields (`login,password,host,port,use_ssl`).
4. Confirm app starts and doctor warnings are non-fatal.
