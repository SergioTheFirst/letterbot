@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo =============================================
echo   Letterbot Premium - Update and Run
echo =============================================

if not exist .git (
    echo [WARN] .git folder not found. Ensure you are in the repository root.
)

where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Git is not available in PATH. Continuing without update.
) else (
    echo Checking working tree cleanliness...
    for /f %%I in ('git status --porcelain') do (
        echo [WARN] Рабочее дерево не чистое. Обновление отменено, чтобы не потерять изменения.
        echo [WARN] Закоммитьте/сохраните изменения и запустите скрипт снова.
        exit /b 1
    )

    echo Fetching origin/main...
    git fetch origin main
    if %ERRORLEVEL% NEQ 0 (
        echo [WARN] Git fetch failed. Continuing with local version.
    ) else (
        echo Resetting to origin/main...
        git reset --hard origin/main
        if %ERRORLEVEL% NEQ 0 (
            echo [WARN] Git reset failed. Continuing with local version.
        )
    )
)

where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found in PATH. Install Python 3.11+ and rerun.
    exit /b 1
)

set "VENV_DIR=%REPO_ROOT%.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo Creating virtual environment...
    python -m venv "%VENV_DIR%"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create .venv
        exit /b 1
    )
)

echo Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Dependency installation failed. Continuing with existing environment.
)

echo Running doctor checks (warning-first)...
"%VENV_PY%" -m mailbot_v26.doctor --config-dir "%REPO_ROOT%mailbot_v26\config"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Doctor found issues. Startup continues in non-strict mode.
)

echo Starting Letterbot...
"%VENV_PY%" -m mailbot_v26.start --config-dir "%REPO_ROOT%mailbot_v26\config"
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" (
    echo Letterbot finished.
) else (
    echo [ERROR] Letterbot terminated with errors.
)

pause
exit /b %RUN_EXIT%
