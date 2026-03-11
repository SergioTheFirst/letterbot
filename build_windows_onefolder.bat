@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
cd /d "%REPO_ROOT%"

echo =============================================
echo   LetterBot.ru - Windows Build (one-folder)
echo =============================================

set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo ERROR: .venv not found. Run letterbot.bat first.
    exit /b 1
)

"%VENV_PY%" -m PyInstaller --version >nul 2>nul
if errorlevel 1 (
    echo ERROR: PyInstaller not available in .venv.
    echo Install with: "%VENV_PY%" -m pip install -r requirements-build.txt
    exit /b 1
)

"%VENV_PY%" -m PyInstaller pyinstaller.spec --noconfirm --clean --distpath "%REPO_ROOT%\dist" --workpath "%REPO_ROOT%\build" --specpath "%REPO_ROOT%"
if errorlevel 1 (
    echo ERROR: PyInstaller build failed.
    exit /b 1
)

set "DIST_DIR=%REPO_ROOT%\dist\Letterbot"
if not exist "%DIST_DIR%" (
    echo ERROR: dist\Letterbot not found after build.
    exit /b 1
)

if exist "%REPO_ROOT%\mailbot_v26\config\settings.ini.example" (
    copy /Y "%REPO_ROOT%\mailbot_v26\config\settings.ini.example" "%DIST_DIR%\mailbot_v26\config\settings.ini.example" >nul
) else (
    echo WARNING: mailbot_v26\config\settings.ini.example not found.
)

if exist "%REPO_ROOT%\mailbot_v26\config\accounts.ini.example" (
    copy /Y "%REPO_ROOT%\mailbot_v26\config\accounts.ini.example" "%DIST_DIR%\mailbot_v26\config\accounts.ini.example" >nul
) else (
    echo WARNING: mailbot_v26\config\accounts.ini.example not found.
)

if exist "%REPO_ROOT%\README_QUICKSTART_WINDOWS.md" (
    copy /Y "%REPO_ROOT%\README_QUICKSTART_WINDOWS.md" "%DIST_DIR%\README_QUICKSTART_WINDOWS.md" >nul
)

if exist "%REPO_ROOT%\run_dist.bat" (
    copy /Y "%REPO_ROOT%\run_dist.bat" "%DIST_DIR%\run.bat" >nul
) else (
    echo WARNING: run_dist.bat not found.
)

if exist "%REPO_ROOT%\docs\UPGRADE.md" (
    copy /Y "%REPO_ROOT%\docs\UPGRADE.md" "%DIST_DIR%\UPGRADE.md" >nul
) else (
    echo WARNING: docs\UPGRADE.md not found.
)

if exist "%REPO_ROOT%\docs\SMARTSCREEN.md" (
    copy /Y "%REPO_ROOT%\docs\SMARTSCREEN.md" "%DIST_DIR%\SMARTSCREEN.md" >nul
) else (
    echo WARNING: docs\SMARTSCREEN.md not found.
)

if exist "%REPO_ROOT%\CHANGELOG.md" (
    copy /Y "%REPO_ROOT%\CHANGELOG.md" "%DIST_DIR%\CHANGELOG.md" >nul
) else (
    echo WARNING: CHANGELOG.md not found.
)

"%VENV_PY%" -c "from pathlib import Path; import json; from mailbot_v26.integrity import compute_manifest, manifest_ignore_paths; root=Path(r'%DIST_DIR%'); ignored=set(manifest_ignore_paths()) | {'manifest.sha256.json'}; manifest={k:v for k,v in compute_manifest(root).items() if k not in ignored}; (root/'manifest.sha256.json').write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')"
if errorlevel 1 (
    echo ERROR: Failed to write manifest.sha256.json
    exit /b 1
)

call "%REPO_ROOT%\verify_dist.bat"
if errorlevel 1 (
    exit /b 1
)

echo =============================================
echo   Build complete: %DIST_DIR%
echo =============================================
