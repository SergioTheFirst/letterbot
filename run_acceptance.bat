@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Acceptance
echo =============================================

echo Checking virtual environment...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    echo ERROR: .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

call "%REPO_ROOT%.venv\Scripts\activate.bat"
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

python -m mailbot_v26 --version
if errorlevel 1 goto FAIL

python -m mailbot_v26 doctor
if errorlevel 1 goto FAIL

python -m mailbot_v26 validate-config
if errorlevel 1 goto FAIL

python -m pytest -q
if errorlevel 1 goto FAIL

echo ACCEPTANCE OK
exit /b 0

:FAIL
echo ACCEPTANCE FAILED
exit /b 1
endlocal
