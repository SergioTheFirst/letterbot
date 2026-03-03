@echo off
setlocal EnableExtensions
set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
cd /d "%REPO_ROOT%"
set "VENV_PY=%REPO_ROOT%\.venv\Scripts\python.exe"

if not exist "%VENV_PY%" (
    echo VERIFY_DIST FAIL: .venv\Scripts\python.exe not found. Run install_and_run.bat first.
    exit /b 1
)

"%VENV_PY%" -m mailbot_v26.tools.verify_dist "%REPO_ROOT%\dist\Letterbot"
set "VERIFY_EXIT=%ERRORLEVEL%"
exit /b %VERIFY_EXIT%
