@echo off
setlocal
set "REPO_ROOT=%~dp0"
set "CONFIG_DIR=%REPO_ROOT%mailbot_v26\config"

if not exist "%CONFIG_DIR%" (
    echo ERROR: Config folder not found: %CONFIG_DIR%
    exit /b 1
)

explorer "%CONFIG_DIR%"
endlocal
