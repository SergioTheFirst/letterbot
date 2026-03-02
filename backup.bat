@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
set "CONFIG_DIR=%REPO_ROOT%"
if "%CONFIG_DIR:~-1%"=="\" set "CONFIG_DIR=%CONFIG_DIR:~0,-1%"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Backup
echo =============================================

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
set "RUN_PY=%VENV_PY%"

if not exist "%RUN_PY%" (
    echo [WARN] .venv Python not found. Falling back to system python.
    set "RUN_PY=python"
)

"%RUN_PY%" --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python not found. Please run letterbot.bat first.
    exit /b 1
)

"%RUN_PY%" -m mailbot_v26 backup
set "EXITCODE=%ERRORLEVEL%"

echo =============================================
echo   Backup completed with code %EXITCODE%
echo =============================================
exit /b %EXITCODE%
endlocal
