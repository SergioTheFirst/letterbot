@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Windows Build (one-folder)
echo =============================================

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo ERROR: .venv not found. Run install_and_run.bat first.
    exit /b 1
)

"%VENV_PY%" -m PyInstaller --version >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: PyInstaller not available in .venv.
    echo Install with: "%VENV_PY%" -m pip install -r requirements-build.txt
    exit /b 1
)

"%VENV_PY%" -m PyInstaller pyinstaller.spec --noconfirm --clean --distpath "%REPO_ROOT%dist" --workpath "%REPO_ROOT%build" --specpath "%REPO_ROOT%"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: PyInstaller build failed.
    exit /b 1
)

set "DIST_DIR=%REPO_ROOT%dist\MailBot"
if not exist "%DIST_DIR%" (
    echo ERROR: dist\MailBot not found after build.
    exit /b 1
)

if exist "%REPO_ROOT%config.example.yaml" (
    copy /Y "%REPO_ROOT%config.example.yaml" "%DIST_DIR%\config.example.yaml" >nul
) else (
    echo WARNING: config.example.yaml not found in repo root.
)

if exist "%REPO_ROOT%README_QUICKSTART_WINDOWS.md" (
    copy /Y "%REPO_ROOT%README_QUICKSTART_WINDOWS.md" "%DIST_DIR%\README_QUICKSTART_WINDOWS.md" >nul
)

if exist "%REPO_ROOT%run_dist.bat" (
    copy /Y "%REPO_ROOT%run_dist.bat" "%DIST_DIR%\run.bat" >nul
) else (
    echo WARNING: run_dist.bat not found.
)

if exist "%REPO_ROOT%docs\UPGRADE.md" (
    copy /Y "%REPO_ROOT%docs\UPGRADE.md" "%DIST_DIR%\UPGRADE.md" >nul
) else (
    echo WARNING: docs\UPGRADE.md not found.
)

if exist "%REPO_ROOT%docs\SMARTSCREEN.md" (
    copy /Y "%REPO_ROOT%docs\SMARTSCREEN.md" "%DIST_DIR%\SMARTSCREEN.md" >nul
) else (
    echo WARNING: docs\SMARTSCREEN.md not found.
)

if exist "%REPO_ROOT%CHANGELOG.md" (
    copy /Y "%REPO_ROOT%CHANGELOG.md" "%DIST_DIR%\CHANGELOG.md" >nul
) else (
    echo WARNING: CHANGELOG.md not found.
)

"%VENV_PY%" -c "from pathlib import Path; import json; from mailbot_v26.integrity import compute_manifest; root=Path(r'%DIST_DIR%'); manifest=compute_manifest(root); manifest.pop('config.yaml', None); manifest.pop('manifest.sha256.json', None); (root/'manifest.sha256.json').write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to write manifest.sha256.json
    exit /b 1
)


call "%REPO_ROOT%verify_dist.bat"
if %ERRORLEVEL% NEQ 0 (
    exit /b 1
)

echo =============================================
echo   Build complete: %DIST_DIR%
echo =============================================
