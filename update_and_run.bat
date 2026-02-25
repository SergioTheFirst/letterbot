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
    echo [WARN] Git is not available in PATH. Continuing without update.
) else (
    echo Fetching repo status...
    git status -sb
    if %ERRORLEVEL% NEQ 0 (
        echo [WARN] Git status failed. Continuing without blocking startup.
    )

    echo Pulling latest changes...
    git pull
    if %ERRORLEVEL% NEQ 0 (
        echo [WARN] Git pull failed ^(no tracking/offline/conflict^). Continuing with local version.
    )
)

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

echo Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Dependency installation failed. Continuing with existing environment.
)

echo Starting Letterbot via run_mailbot.bat...
call "%REPO_ROOT%run_mailbot.bat"
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" (
    echo Letterbot finished.
) else if "%RUN_EXIT%"=="2" (
    echo [INFO] Letterbot setup is incomplete. accounts.ini was opened for editing.
    echo [INFO] Finish required IMAP fields, then run update_and_run.bat again.
    set "RUN_EXIT=0"
) else (
    echo [ERROR] Letterbot terminated with errors.
)

pause
exit /b %RUN_EXIT%
