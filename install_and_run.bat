@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1

rem Move to repo root
cd /d "%~dp0"
set REPO_ROOT=%CD%

echo =============================================
echo   MailBot Premium v26 - Install and Run
echo =============================================

echo Checking repository status...
if exist .git (
    git --version >nul 2>&1
    if %ERRORLEVEL%==0 (
        for /f "usebackq delims=" %%I in (`git rev-parse --abbrev-ref HEAD 2^>nul`) do set GIT_BRANCH=%%I
        if defined GIT_BRANCH echo Git branch: %GIT_BRANCH%
    ) else (
        echo Git repository detected but git is not available.
    )
) else (
    echo Warning: .git not found, continuing without git metadata.
)

echo Checking Python 3.10+...
python - <<"PYVER"
import sys
major, minor = sys.version_info[:2]
if (major, minor) < (3, 10):
    print("ERROR: Python 3.10+ is required.")
    sys.exit(1)
print(f"Python version OK: {major}.{minor}")
PYVER
if %ERRORLEVEL% NEQ 0 (
    pause
    exit /b 1
)

echo Creating virtual environment if missing...
if not exist "%REPO_ROOT%\venv\Scripts\activate.bat" (
    python -m venv "%REPO_ROOT%\venv"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        pause
        exit /b 1
    )
)

call "%REPO_ROOT%\venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    pause
    exit /b 1
)

echo Upgrading pip...
python -m pip install --upgrade pip
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: pip upgrade failed.
    pause
    exit /b 1
)

echo Installing dependencies...
pip install -r "%REPO_ROOT%\mailbot_v26\requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
    pause
    exit /b 1
)

echo Ensuring data directory exists...
if not exist "%REPO_ROOT%\mailbot_v26\data" (
    mkdir "%REPO_ROOT%\mailbot_v26\data"
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
