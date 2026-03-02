@echo off
setlocal EnableExtensions
set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
set "CONFIG_DIR=%REPO_ROOT%"
cd /d "%REPO_ROOT%"
rem Конфигурационные файлы: settings.ini и accounts.ini в корне репозитория.

if not exist "%CONFIG_DIR%" (
    echo ERROR: Config folder not found: %CONFIG_DIR%
    exit /b 1
)

if not exist "%REPO_ROOT%\accounts.ini" (
    echo [WARN] accounts.ini not found in %REPO_ROOT%
    echo [HINT] Create accounts.ini in repo root and run letterbot.bat once for auto-bootstrap.
)

explorer "%CONFIG_DIR%"
endlocal
