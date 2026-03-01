@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "REPO_ROOT=%CD%"
set "CONFIG_DIR=%REPO_ROOT%\mailbot_v26\config"
set "VENV_DIR=%REPO_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "SETTINGS_FILE=%CONFIG_DIR%\settings.ini"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"

echo =============================================
echo   Letterbot Premium - One-click Run
echo =============================================
echo Repo root: %REPO_ROOT%
echo Config dir: %CONFIG_DIR%

if not exist "%VENV_PY%" (
    echo [1/6] Creating .venv...
    python -m venv "%VENV_DIR%"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create .venv
        exit /b 1
    )
)

echo [2/6] Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%\requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Dependency installation failed.
    exit /b 1
)

echo [3/6] Initializing/migrating config in %CONFIG_DIR%...
"%VENV_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
"%VENV_PY%" -m mailbot_v26 migrate-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

:CONFIG_READY_CHECK
"%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
if %ERRORLEVEL% EQU 2 (
    echo [ACTION REQUIRED] Config is not ready. Opening accounts.ini...
    start /wait notepad.exe "%ACCOUNTS_FILE%"
    goto :CONFIG_READY_CHECK
)
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

echo [4/6] Doctor (warning-first)...
"%VENV_PY%" -m mailbot_v26 doctor --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 echo [WARN] doctor reported issues.

echo [5/6] Validate-config (warning-first)...
"%VENV_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 echo [WARN] validate-config reported issues.

set "WEB_ENABLED=0"
for /f %%I in ('"%VENV_PY%" -c "from pathlib import Path; from mailbot_v26.config.paths import resolve_config_paths; from mailbot_v26.web_observability.app import _resolve_yaml_config_path, _load_web_ui_settings; cfg=Path(r'%CONFIG_DIR%');\
try:\
 p=_resolve_yaml_config_path(None,cfg); s=_load_web_ui_settings(p); print('1' if s.enabled else '0')\
except Exception:\
 print('0')"') do set "WEB_ENABLED=%%I"

if "%WEB_ENABLED%"=="1" (
    echo [6/6] Starting web UI in background...
    start "Letterbot Web UI" /B "%VENV_PY%" -m mailbot_v26.web_observability.app --config "%CONFIG_DIR%"
) else (
    echo [6/6] web_ui.enabled=false (or config.yaml unavailable), skipping Web UI start.
)

echo Starting Letterbot runtime...
"%VENV_PY%" -m mailbot_v26.start --config-dir "%CONFIG_DIR%"
exit /b %ERRORLEVEL%
