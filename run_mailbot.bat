@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "REPO_ROOT=%CD%"
set "CONFIG_DIR=%REPO_ROOT%\mailbot_v26\config"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"
set "KEYS_FILE=%CONFIG_DIR%\keys.ini"
set "INI_FILE=%CONFIG_DIR%\config.ini"
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
echo Repo: %REPO_ROOT%
echo Config dir: %CONFIG_DIR%

echo Python version:
"%VENV_PY%" -c "import sys; print(sys.version)"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not available in the virtual environment.
    exit /b 1
)

set "MISSING_CONFIG=0"
call :require_config "%INI_FILE%" "config.ini" "python -m mailbot_v26 init-config"
call :require_config "%ACCOUNTS_FILE%" "accounts.ini" "python -m mailbot_v26 init-config"
call :require_config "%KEYS_FILE%" "keys.ini" "python -m mailbot_v26 init-config"
if "%MISSING_CONFIG%"=="1" (
    echo.
    echo [ERROR] Required user config is missing. Letterbot did not start.
    exit /b 1
)

if not exist "%YAML_FILE%" (
    echo [WARN] Optional config.yaml not found at "%YAML_FILE%".
    echo [WARN] Startup will use INI config from mailbot_v26\config\*.ini.
)

echo Running doctor checks...
"%VENV_PY%" -m mailbot_v26.doctor
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Doctor reported critical issues.
    exit /b 1
)

echo Running config validation...
"%VENV_PY%" -m mailbot_v26 validate-config
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Config validation failed.
    echo [HINT] Fix files in mailbot_v26\config and re-run.
    exit /b 1
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start
exit /b %ERRORLEVEL%

:require_config
if exist "%~1" (
    exit /b 0
)
set "MISSING_CONFIG=1"
echo [ERROR] Missing required file: "%~1"
echo [HINT] Fill this file and restart.
echo [HINT] To create templates: %~3
exit /b 0
