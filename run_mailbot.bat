@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "REPO_ROOT=%CD%"
set "CONFIG_DIR=%REPO_ROOT%\mailbot_v26\config"
set "SETTINGS_FILE=%CONFIG_DIR%\settings.ini"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"

set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] .venv not found: "%VENV_PY%"
    echo [HINT] Run install_and_run.bat
    exit /b 1
)

echo =============================================
echo   Letterbot Premium - Run
echo =============================================

REM === ONBOARDING GATE ===
REM Required files are created on first run.
set "FIRST_RUN=0"
if not exist "%SETTINGS_FILE%" set "FIRST_RUN=1"
if not exist "%ACCOUNTS_FILE%" set "FIRST_RUN=1"

if "%FIRST_RUN%"=="1" (
    echo [SETUP] Создаю шаблоны конфигурации...
    "%VENV_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >nul 2>nul
)

if not exist "%ACCOUNTS_FILE%" (
    echo [SETUP] accounts.ini не найден. Создаю шаблон и открываю Блокнот...
    "%VENV_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >nul 2>nul
    start /wait notepad.exe "%ACCOUNTS_FILE%"
)

"%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
if %ERRORLEVEL% EQU 2 (
    echo.
    echo =============================================
    echo   LETTERBOT — ТРЕБУЕТСЯ НАСТРОЙКА
    echo =============================================
    echo  Заполните только обязательные поля IMAP-аккаунта:
    echo    - login, password, host, port, use_ssl
    echo  В INI кавычки не нужны.
    echo  Для Windows-домена: login = HQ\MedvedevSS
    echo.
    echo  Сейчас откроется accounts.ini. После сохранения закройте Блокнот.
    start /wait notepad.exe "%ACCOUNTS_FILE%"

    "%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
    if %ERRORLEVEL% EQU 2 (
        echo [ERROR] Конфигурация всё ещё не готова. Бот не запущен.
        exit /b 2
    )
)
REM === END ONBOARDING GATE ===

echo Running doctor checks ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26.doctor --config-dir "mailbot_v26\config"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues in non-strict mode.
)

echo Running config validation ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] validate-config reported warnings. Startup continues.
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start --config-dir "mailbot_v26\config"
exit /b %ERRORLEVEL%
