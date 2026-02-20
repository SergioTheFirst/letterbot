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
echo   Letterbot Premium - Update and Run
echo =============================================

echo Checking Git...
where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Git is not available in PATH. Please install Git and retry.
    exit /b 1
)

echo Fetching repo status...
git status -sb
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Git status failed.
    exit /b 1
)

echo Pulling latest changes...
git pull
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Git pull failed.
    exit /b 1
)

echo Checking virtual environment...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    echo ERROR: .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
call "%REPO_ROOT%.venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

set "REQ_FILE=%REPO_ROOT%requirements.txt"
if not exist "%REQ_FILE%" (
    echo ERROR: requirements.txt not found in repo root.
    exit /b 1
)

echo Installing dependencies from "%REQ_FILE%"...
"%VENV_PY%" -m pip install -r "%REQ_FILE%"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
    exit /b 1
)

set "ACCOUNTS_FILE=%REPO_ROOT%mailbot_v26\config\accounts.ini"
if not exist "%ACCOUNTS_FILE%" (
    echo Running init-config to create templates...
    "%VENV_PY%" -m mailbot_v26 init-config
    color 0C
    echo =============================================
    echo   CONFIGURATION REQUIRED
    echo   Заполни mailbot_v26\config\accounts.ini
    echo   затем запусти update_and_run.bat снова.
    echo =============================================
    exit /b 1
)

echo Running doctor checks...
"%VENV_PY%" -m mailbot_v26 doctor
if %ERRORLEVEL% NEQ 0 (
    color 0C
    echo =============================================
    echo   CRITICAL DOCTOR ISSUES DETECTED
    echo   Letterbot will NOT start polling.
    echo   Please review the doctor report and fix IMAP/Telegram/DB issues.
    echo =============================================
    exit /b 1
)

echo Running config validation...
"%VENV_PY%" -m mailbot_v26 validate-config
if %ERRORLEVEL% NEQ 0 (
    color 0C
    echo =============================================
    echo   CONFIG VALIDATION FAILED
    echo   Please fix config files and retry.
    echo =============================================
    exit /b 1
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Letterbot terminated with errors.
) else (
    echo Letterbot finished.
)

echo =============================================
echo   DONE. Close this window or press a key.
echo =============================================
pause
endlocal
