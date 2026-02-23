@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"

echo =============================================
echo   Letterbot Premium - Run
echo =============================================

set "VENV_PY=%~dp0.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo ERROR: .venv not found. Запустите install_and_run.bat
    exit /b 1
)

echo VENV_PY: "%VENV_PY%"
echo Python version:
"%VENV_PY%" -c "import sys; print(sys.version)"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not available in the virtual environment.
    exit /b 1
)

echo Running doctor checks...
"%VENV_PY%" -m mailbot_v26.doctor
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: doctor checks failed.
    exit /b 1
)

echo Running config validation...
"%VENV_PY%" -m mailbot_v26 validate-config
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: config validation failed.
    exit /b 1
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start
exit /b %ERRORLEVEL%
