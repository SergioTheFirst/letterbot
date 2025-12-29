@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"
set "REPO_ROOT=%~dp0"
pushd "%REPO_ROOT%"

echo =============================================
echo   MailBot Premium v26 - Install and Run
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

echo Creating virtual environment if missing...
if not exist "%REPO_ROOT%venv\Scripts\activate.bat" (
    python -m venv "%REPO_ROOT%venv"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        popd
        exit /b 1
    )
)

call "%REPO_ROOT%venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    popd
    exit /b 1
)

echo Upgrading pip...
python -m pip install --upgrade pip
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: pip upgrade failed.
    popd
    exit /b 1
)

set "REQ_FILE="
if exist "%REPO_ROOT%requirements.txt" (
    set "REQ_FILE=%REPO_ROOT%requirements.txt"
) else if exist "%REPO_ROOT%mailbot_v26\requirements.txt" (
    set "REQ_FILE=%REPO_ROOT%mailbot_v26\requirements.txt"
) else (
    echo ERROR: requirements.txt not found in repo root or mailbot_v26\.
    popd
    exit /b 1
)

echo Installing dependencies from %REQ_FILE% ...
pip install -r "%REQ_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
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
