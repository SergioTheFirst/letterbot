@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Tests
echo =============================================

echo Checking virtual environment...
if not exist "%REPO_ROOT%\.venv\Scripts\python.exe" (
    echo ERROR: .venv not found. Please run letterbot.bat first.
    exit /b 1
)

set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"

echo Running smoke and full test suite...
"%VENV_PY%" -m pytest -q
set "EXITCODE=%ERRORLEVEL%"

echo =============================================
echo   Tests completed with code %EXITCODE%
echo =============================================
exit /b %EXITCODE%
endlocal
