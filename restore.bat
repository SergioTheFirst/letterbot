@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   MailBot Premium - Restore
echo =============================================

echo Checking virtual environment...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    echo ERROR: .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

call "%REPO_ROOT%.venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

if "%~1"=="" (
    echo ERROR: Please provide a backup archive path.
    echo Usage: restore.bat ^<path_to_backup.zip^>
    exit /b 1
)

python -m mailbot_v26 restore "%~1"
set EXITCODE=%ERRORLEVEL%

echo =============================================
echo   Restore completed with code %EXITCODE%
echo =============================================
exit /b %EXITCODE%
endlocal
