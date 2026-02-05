@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"

echo =============================================
echo   MailBot Premium - Run
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

set "CONFIG_FILE=%~dp0config.yaml"
set "CONFIG_EXAMPLE=%~dp0config.example.yaml"
if not exist "%CONFIG_FILE%" (
    if exist "%CONFIG_EXAMPLE%" (
        copy /Y "%CONFIG_EXAMPLE%" "%CONFIG_FILE%" >nul
        echo =============================================
        echo   CONFIGURATION REQUIRED
        echo   Откройте config.yaml и заполните значения в кавычках.
        echo =============================================
        notepad "%CONFIG_FILE%"
    ) else (
        echo ERROR: config.example.yaml not found in repo root.
    )
    exit /b 1
)

echo Running health checks...
"%VENV_PY%" -m mailbot_v26 doctor
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: doctor checks failed.
    exit /b 1
)

"%VENV_PY%" -m mailbot_v26 validate-config
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: config validation failed.
    exit /b 1
)

echo Starting MailBot...
"%VENV_PY%" -m mailbot_v26.start
exit /b %ERRORLEVEL%
