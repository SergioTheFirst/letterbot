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
    git diff --quiet --ignore-submodules HEAD --
    if %ERRORLEVEL% NEQ 0 (
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

set "VENV_PY=%REPO_ROOT%.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
    echo [ERROR] .venv not found. Please run install_and_run.bat first.
    exit /b 1
)

echo Installing dependencies...
"%VENV_PY%" -m pip install -r "%REPO_ROOT%requirements.txt"
if %ERRORLEVEL% NEQ 0 (
    echo [WARN] Dependency installation failed. Continuing with existing environment.
)

echo Starting Letterbot via run_mailbot.bat...
call "%REPO_ROOT%run_mailbot.bat"
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" (
    echo Letterbot finished.
) else (
    echo [ERROR] Letterbot terminated with errors.
)

pause
exit /b %RUN_EXIT%
