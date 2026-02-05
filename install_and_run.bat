@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

if not exist .git (
    echo WARNING: .git folder not found. Ensure you are in the repository root.
)

echo =============================================
echo   MailBot Premium - Install and Run
echo =============================================

echo Creating virtual environment if missing...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    python -m venv "%REPO_ROOT%.venv"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        exit /b 1
    )
)

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
call "%REPO_ROOT%.venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

echo Upgrading pip...
"%VENV_PY%" -m pip install --upgrade pip
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: pip upgrade failed.
    exit /b 1
)

echo Installing dependencies from requirements.txt...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
    exit /b 1
)

call "%REPO_ROOT%run_mailbot.bat"
exit /b %ERRORLEVEL%
