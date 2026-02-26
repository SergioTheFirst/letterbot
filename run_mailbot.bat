@echo off
setlocal enableextensions enabledelayedexpansion
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
    echo  Чтобы отменить: нажмите Ctrl+C в этом окне.
    set /a CONFIG_READY_ATTEMPTS=0
    :CONFIG_READY_LOOP
    set /a CONFIG_READY_ATTEMPTS+=1
    if !CONFIG_READY_ATTEMPTS! GTR 20 (
        echo [ERROR] Конфигурация всё ещё не готова после 20 попыток. Бот не запущен.
        exit /b 2
    )
    start /wait notepad.exe "%ACCOUNTS_FILE%"

    "%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
    if %ERRORLEVEL% EQU 2 (
        echo [WARN] Попытка !CONFIG_READY_ATTEMPTS! из 20: обязательные поля ещё не заполнены.
        goto :CONFIG_READY_LOOP
    )
)
REM === END ONBOARDING GATE ===

echo Running doctor checks ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26.doctor --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues in non-strict mode.
)

echo Running config validation ^(warning-first^)...
"%VENV_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] validate-config reported warnings. Startup continues.
)

for /f "tokens=1,2 delims= " %%A in ('"%VENV_PY%" -c "from pathlib import Path; from mailbot_v26.config_loader import load_web_config; web=load_web_config(Path(r'%CONFIG_DIR%')); print(str(web.host)+' '+str(web.port))"') do (
    set "WEB_HOST=%%A"
    set "WEB_PORT=%%B"
)
if defined WEB_HOST if defined WEB_PORT (
    echo Starting web on http://%WEB_HOST%:%WEB_PORT%
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start --config-dir "%CONFIG_DIR%"
exit /b %ERRORLEVEL%
