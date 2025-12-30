@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   MailBot Premium - Tests
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

echo Running smoke and full test suite...
python -m pytest -q
set EXITCODE=%ERRORLEVEL%

echo =============================================
echo   Tests completed with code %EXITCODE%
echo =============================================
exit /b %EXITCODE%
endlocal
