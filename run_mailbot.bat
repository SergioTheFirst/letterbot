@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"
set "REPO_ROOT=%~dp0"
pushd "%REPO_ROOT%"

echo =============================================
echo   MailBot Premium v26 - Run
echo =============================================

echo Checking Python 3.10+...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not available in PATH.
    popd
    exit /b 1
)
python -c "import sys; major, minor = sys.version_info[:2]; print(f'Python version detected: {major}.{minor}'); sys.exit(0 if (major, minor) >= (3, 10) else 1)"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python 3.10 or higher is required.
    popd
    exit /b 1
)

echo Checking virtual environment...
if not exist "%REPO_ROOT%venv\Scripts\activate.bat" (
    echo ERROR: venv not found. Please run install_and_run.bat first.
    popd
    exit /b 1
)

call "%REPO_ROOT%venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    popd
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
popd
pause
endlocal
