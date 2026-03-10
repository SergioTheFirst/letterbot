# Windows Release Checklist

## Build
1. Ensure `.venv` contains runtime and build dependencies.
2. Run `build_windows_onefolder.bat`.
3. Confirm output folder `dist\Letterbot`.
4. Confirm required files:
   - `Letterbot.exe`
   - `run.bat`
   - `mailbot_v26\config\settings.ini.example`
   - `mailbot_v26\config\accounts.ini.example`
   - `README_QUICKSTART_WINDOWS.md`
   - `manifest.sha256.json`
5. Run `verify_dist.bat`.

## Package
1. Create the ZIP: `Compress-Archive dist/Letterbot dist/Letterbot.zip`.
2. Publish artifact name `Letterbot-windows-onefolder`.

## Smoke
1. On a clean machine, extract `Letterbot.zip`.
2. Open the extracted `Letterbot` folder and run `run.bat`.
3. Fill required `accounts.ini` fields (`login,password,host,port,use_ssl`).
4. Confirm startup checks pass and the doctor report is non-fatal.
