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
echo   MailBot Premium - Run
echo =============================================

echo Checking virtual environment...
if not exist "%REPO_ROOT%.venv\Scripts\activate.bat" (
    echo ERROR: .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
echo Checking Python 3.10+...
"%VENV_PY%" --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not available in the virtual environment.
    exit /b 1
)
"%VENV_PY%" -c "import sys; major, minor = sys.version_info[:2]; print(f'Python version detected: {major}.{minor}'); sys.exit(0 if (major, minor) >= (3, 10) else 1)"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python 3.10 or higher is required.
    exit /b 1
)

call "%REPO_ROOT%.venv\Scripts\activate.bat"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate virtual environment.
    exit /b 1
)

set "CONFIG_FILE=%REPO_ROOT%config.yaml"
set "CONFIG_EXAMPLE=%REPO_ROOT%config.example.yaml"
if not exist "%CONFIG_FILE%" (
    if exist "%CONFIG_EXAMPLE%" (
        copy /Y "%CONFIG_EXAMPLE%" "%CONFIG_FILE%" >nul
        echo =============================================
        echo   CONFIGURATION REQUIRED
        echo   Откройте config.yaml и заполните значения.
        echo =============================================
        notepad "%CONFIG_FILE%"
    ) else (
        echo ERROR: config.example.yaml not found in repo root.
    )
    exit /b 1
)

echo Starting MailBot...
"%VENV_PY%" -m mailbot_v26.start
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
