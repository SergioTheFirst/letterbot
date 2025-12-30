@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

REM Determine repo root and switch drive
set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

if not exist .git (
    echo WARNING: .git folder not found. Ensure you are in the repository root.
)

echo =============================================
echo   MailBot Premium - Install and Run
echo =============================================

echo Checking Python 3.10+...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not available in PATH.
    exit /b 1
)
python -c "import sys; major, minor = sys.version_info[:2]; print(f'Python version detected: {major}.{minor}'); sys.exit(0 if (major, minor) >= (3, 10) else 1)"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python 3.10 or higher is required.
    exit /b 1
)

echo Creating virtual environment if missing...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    python -m venv "%REPO_ROOT%.venv"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        exit /b 1
    )
)

call "%REPO_ROOT%.venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

echo Upgrading pip...
python -m pip install --upgrade pip
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: pip upgrade failed.
    exit /b 1
)

set "REQ_FILE="
if exist "%REPO_ROOT%mailbot_v26\requirements.txt" (
    set "REQ_FILE=%REPO_ROOT%mailbot_v26\requirements.txt"
) else if exist "%REPO_ROOT%requirements.txt" (
    set "REQ_FILE=%REPO_ROOT%requirements.txt"
) else (
    echo ERROR: requirements.txt not found in repo root or mailbot_v26\.
    exit /b 1
)

echo Installing dependencies from "%REQ_FILE%"...
python -m pip install -r "%REQ_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
    exit /b 1
)

set "ACCOUNTS_FILE=%REPO_ROOT%mailbot_v26\config\accounts.ini"
if not exist "%ACCOUNTS_FILE%" (
    echo Running init-config to create templates...
    python -m mailbot_v26 init-config
    color 0C
    echo =============================================
    echo   CONFIGURATION REQUIRED
    echo   Заполни mailbot_v26\config\accounts.ini
    echo   затем запусти install_and_run.bat снова.
    echo =============================================
    exit /b 1
)

echo Running doctor checks...
python -m mailbot_v26 doctor
if %ERRORLEVEL% NEQ 0 (
    color 0C
    echo =============================================
    echo   CRITICAL DOCTOR ISSUES DETECTED
    echo   MailBot will NOT start polling.
    echo   Please review the doctor report and fix IMAP/Telegram/DB issues.
    echo =============================================
    exit /b 1
)

echo Running config validation...
python -m mailbot_v26 validate-config
if %ERRORLEVEL% NEQ 0 (
    color 0C
    echo =============================================
    echo   CONFIG VALIDATION FAILED
    echo   Please fix config files and retry.
    echo =============================================
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
