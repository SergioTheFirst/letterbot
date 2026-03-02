@echo off
setlocal
set "REPO_ROOT=%~dp0"
set "CONFIG_DIR=%REPO_ROOT%"
if "%CONFIG_DIR:~-1%"=="\" set "CONFIG_DIR=%CONFIG_DIR:~0,-1%"
rem Конфигурационные файлы: settings.ini и accounts.ini в корне репозитория.

if not exist "%CONFIG_DIR%" (
    echo ERROR: Config folder not found: %CONFIG_DIR%
    exit /b 1
)

if not exist "%REPO_ROOT%accounts.ini" (
    echo [WARN] accounts.ini not found in %REPO_ROOT%
    echo [HINT] Create accounts.ini in repo root and run letterbot.bat once for auto-bootstrap.
)

explorer "%CONFIG_DIR%"
endlocal
