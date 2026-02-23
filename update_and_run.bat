@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Update and Run
echo =============================================

if not exist .git (
    echo [WARN] .git folder not found. Ensure you are in the repository root.
)

where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Git is not available in PATH. Please install Git and retry.
    exit /b 1
)

echo Fetching repo status...
git status -sb
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Git status failed.
    exit /b 1
)

echo Pulling latest changes...
git pull
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Git pull failed.
    exit /b 1
)

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

echo Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Dependency installation failed.
    exit /b 1
)

echo Starting Letterbot via run_mailbot.bat...
call "%REPO_ROOT%run_mailbot.bat"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Letterbot terminated with errors.
) else (
    echo Letterbot finished.
)

pause
exit /b %ERRORLEVEL%
