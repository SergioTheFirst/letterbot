@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "REPO_ROOT=%CD%"
set "CONFIG_DIR=%REPO_ROOT%"
set "SETTINGS_FILE=%CONFIG_DIR%\settings.ini"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"
set "SETTINGS_EXAMPLE=%REPO_ROOT%\mailbot_v26\config\settings.ini.example"
set "ACCOUNTS_EXAMPLE=%REPO_ROOT%\mailbot_v26\config\accounts.ini.example"

echo =============================================
echo   Letterbot Premium - Install and Run
echo =============================================
echo Repo root: %REPO_ROOT%
echo Config root: %CONFIG_DIR%

echo [1/5] Creating .venv if missing...
if not exist "%REPO_ROOT%\.venv\Scripts\python.exe" (
    python -m venv "%REPO_ROOT%\.venv"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create virtual environment.
        exit /b 1
    )
)

set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"

echo [2/5] Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%\requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Dependency installation failed.
    exit /b 1
)

echo [3/5] Ensuring root settings.ini/accounts.ini exist...
if not exist "%SETTINGS_FILE%" (
    if exist "%SETTINGS_EXAMPLE%" (
        copy /Y "%SETTINGS_EXAMPLE%" "%SETTINGS_FILE%" >nul
        echo [SETUP] Created %SETTINGS_FILE% from template.
    ) else (
        echo [ERROR] settings.ini.example not found: %SETTINGS_EXAMPLE%
        exit /b 1
    )
)
if not exist "%ACCOUNTS_FILE%" (
    if exist "%ACCOUNTS_EXAMPLE%" (
        copy /Y "%ACCOUNTS_EXAMPLE%" "%ACCOUNTS_FILE%" >nul
        echo [SETUP] Created %ACCOUNTS_FILE% from template.
    ) else (
        echo [ERROR] accounts.ini.example not found: %ACCOUNTS_EXAMPLE%
        exit /b 1
    )
)

echo [4/5] Health checks (warning-first)...
"%VENV_PY%" -m mailbot_v26.doctor --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues.
)

echo [5/5] Starting services (mail loop + web UI + schedulers + Telegram inbound/outbound)...
echo Open web UI at: http://127.0.0.1:8787
"%VENV_PY%" -m mailbot_v26.start --config-dir "%CONFIG_DIR%"
exit /b %ERRORLEVEL%
