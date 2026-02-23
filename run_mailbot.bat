@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "REPO_ROOT=%CD%"
set "CONFIG_DIR=%REPO_ROOT%\mailbot_v26\config"
set "SETTINGS_FILE=%CONFIG_DIR%\settings.ini"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"
set "YAML_FILE=%REPO_ROOT%\config.yaml"

set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] .venv not found: "%VENV_PY%"
    echo [HINT] Run install_and_run.bat
    exit /b 1
)

echo =============================================
echo   Letterbot Premium - Run
echo =============================================

if not exist "%SETTINGS_FILE%" (
  echo [WARN] settings.ini not found. Creating defaults...
  "%VENV_PY%" -m mailbot_v26 init-config >nul 2>nul
)
if not exist "%ACCOUNTS_FILE%" (
  echo [WARN] accounts.ini not found. Creating template...
  "%VENV_PY%" -m mailbot_v26 init-config >nul 2>nul
)

echo Running doctor checks ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26 doctor --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues in non-strict mode.
)

echo Running config validation ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] validate-config reported warnings. Startup continues.
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start --config-dir "%CONFIG_DIR%"
exit /b %ERRORLEVEL%
