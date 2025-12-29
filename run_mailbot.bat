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
echo   MailBot Premium v26 - Run
echo =============================================

echo Checking Python 3.10+...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not available in PATH.
    exit /b 1
)
python - <<"PYVER"
import sys
major, minor = sys.version_info[:2]
print(f"Python version detected: {major}.{minor}")
if (major, minor) < (3, 10):
    sys.exit(1)
PYVER
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python 3.10 or higher is required.
    exit /b 1
)

echo Checking virtual environment...
if not exist "%REPO_ROOT%venv\Scripts\activate.bat" (
    echo ERROR: venv not found. Please run install_and_run.bat first.
    exit /b 1
)

call "%REPO_ROOT%venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

echo Starting MailBot...
python -m mailbot_v26
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: MailBot terminated with errors.
) else (
    echo MailBot finished.
)

echo =============================================
echo   DONE. Close this window or press a key.
echo =============================================
pause
endlocal
