@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"

set "EXE_PATH=%~dp0Letterbot.exe"
set "CONFIG_DIR=%~dp0mailbot_v26\config"
set "SETTINGS_FILE=%CONFIG_DIR%\settings.ini"
set "ACCOUNTS_FILE=%CONFIG_DIR%\accounts.ini"
set "SETTINGS_EXAMPLE=%CONFIG_DIR%\settings.ini.example"
set "ACCOUNTS_EXAMPLE=%CONFIG_DIR%\accounts.ini.example"

if not exist "%EXE_PATH%" (
    echo ERROR: Letterbot.exe not found.
    exit /b 1
)

if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"

set "FIRST_RUN=0"
if not exist "%SETTINGS_FILE%" set "FIRST_RUN=1"
if not exist "%ACCOUNTS_FILE%" set "FIRST_RUN=1"

if "%FIRST_RUN%"=="1" (
    echo [SETUP] Bootstrap 2-file config from examples...
    if not exist "%SETTINGS_FILE%" (
        if exist "%SETTINGS_EXAMPLE%" (
            copy /Y "%SETTINGS_EXAMPLE%" "%SETTINGS_FILE%" >nul
        ) else (
            echo ERROR: settings.ini.example not found.
            exit /b 1
        )
    )
    if not exist "%ACCOUNTS_FILE%" (
        if exist "%ACCOUNTS_EXAMPLE%" (
            copy /Y "%ACCOUNTS_EXAMPLE%" "%ACCOUNTS_FILE%" >nul
        ) else (
            echo ERROR: accounts.ini.example not found.
            exit /b 1
        )
    )
)

"%EXE_PATH%" config-ready --config-dir "%CONFIG_DIR%" --verbose
if %ERRORLEVEL% EQU 2 (
    echo.
    echo =============================================
    echo   LETTERBOT — ТРЕБУЕТСЯ НАСТРОЙКА
    echo =============================================
    echo  Заполните обязательные поля IMAP-аккаунта:
    echo    - login, password, host, port, use_ssl
    echo.
    echo  Сейчас откроется accounts.ini. После сохранения закройте Блокнот.
    set /a CONFIG_READY_ATTEMPTS=0
    :CONFIG_READY_LOOP
    set /a CONFIG_READY_ATTEMPTS+=1
    if !CONFIG_READY_ATTEMPTS! GTR 20 (
        echo [ERROR] Конфигурация всё ещё не готова после 20 попыток. Бот не запущен.
        exit /b 2
    )

    start /wait notepad.exe "%ACCOUNTS_FILE%"
    "%EXE_PATH%" config-ready --config-dir "%CONFIG_DIR%" --verbose
    if %ERRORLEVEL% EQU 2 (
        echo [WARN] Попытка !CONFIG_READY_ATTEMPTS! из 20: обязательные поля ещё не заполнены.
        goto :CONFIG_READY_LOOP
    )
)

echo Running doctor checks ^(warning-first^)...
"%EXE_PATH%" doctor --config-dir "%CONFIG_DIR%"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues in non-strict mode.
)

echo Starting Letterbot...
"%EXE_PATH%" --config-dir "%CONFIG_DIR%"
exit /b %ERRORLEVEL%
