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
REM If either required config file is missing, run init-config to create templates.
REM Then check for unconfigured placeholders and stop cleanly if found.

set "FIRST_RUN=0"
if not exist "%SETTINGS_FILE%" set "FIRST_RUN=1"
if not exist "%ACCOUNTS_FILE%" set "FIRST_RUN=1"

if "%FIRST_RUN%"=="1" (
    echo [SETUP] Создаю шаблоны конфигурации...
    "%VENV_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >nul 2>nul
)

REM Check for unconfigured state: CHANGE_ME in accounts.ini means user hasn't filled in credentials
findstr /m "CHANGE_ME" "%ACCOUNTS_FILE%" >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    echo.
    echo =============================================
    echo   LETTERBOT — ТРЕБУЕТСЯ НАСТРОЙКА
    echo =============================================
    echo.
    echo  Шаг 1. Сейчас откроется файл accounts.ini в Блокноте.
    echo  Шаг 2. Замените CHANGE_ME на реальные значения:
    echo         - login      = ваш email (например user@mail.ru)
    echo         - password   = пароль приложения (не основной пароль!)
    echo         - host       = IMAP-сервер (например imap.mail.ru)
    echo         - bot_token  = токен Telegram-бота (от @BotFather)
    echo         - telegram_chat_id = ваш Telegram ID (от @userinfobot)
    echo  Шаг 3. Сохраните файл и запустите run_mailbot.bat снова.
    echo.
    echo  Подробная инструкция: README_QUICKSTART_WINDOWS.md
    echo.
    start notepad.exe "%ACCOUNTS_FILE%"
    echo [OK] Letterbot ждёт настройки. Это нормально — не ошибка.
    exit /b 0
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
