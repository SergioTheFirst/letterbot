@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"

echo [1/3] Creating .venv if missing...
if not exist "%~dp0.venv\Scripts\python.exe" (
    python -m venv "%~dp0.venv"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to create virtual environment.
        exit /b 1
    )
)

set "VENV_PY=%~dp0.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo ERROR: .venv is missing. Запустите install_and_run.bat
    exit /b 1
)

echo [2/3] Installing dependencies...
"%VENV_PY%" -m pip install -r "%~dp0requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Dependency installation failed.
    exit /b 1
)

echo [3/3] Starting Letterbot...
call "%~dp0run_mailbot.bat"
exit /b %ERRORLEVEL%
