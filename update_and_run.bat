@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
set "CONFIG_DIR=%REPO_ROOT%"
cd /d "%REPO_ROOT%"

set "LOG_DIR=%REPO_ROOT%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\update_and_run.log"
>"%LOG_FILE%" echo [%DATE% %TIME%] update_and_run started

call :log =============================================
call :log   Letterbot Premium - Update and Run
call :log =============================================
call :log "Virtual environment: %REPO_ROOT%\.venv"
call :log "Log file: %LOG_FILE%"

if not exist "%REPO_ROOT%\.git" (
    call :log [WARN] .git folder not found. Ensure you are in the repository root.
)

where git >nul 2>&1
if errorlevel 1 (
    call :log [WARN] Git is not available in PATH. Continuing without update.
) else (
    call :log Checking working tree cleanliness...
    for /f %%I in ('git status --porcelain') do (
        call :log [WARN] Рабочее дерево не чистое. Обновление отменено, чтобы не потерять изменения.
        call :log [WARN] Закоммитьте/сохраните изменения и запустите скрипт снова.
        call :finish 1
        exit /b 1
    )

    call :log Fetching origin/main...
    git fetch origin main >>"%LOG_FILE%" 2>&1
    if errorlevel 1 (
        call :log [WARN] Git fetch failed. Continuing with local version.
    ) else (
        call :log Resetting to origin/main...
        git reset --hard origin/main >>"%LOG_FILE%" 2>&1
        if errorlevel 1 (
            call :log [WARN] Git reset failed. Continuing with local version.
        )
    )
)

where python >nul 2>&1
if errorlevel 1 (
    call :log [ERROR] Python not found in PATH. Install Python 3.11+ and rerun.
    call :finish 1
    exit /b 1
)

for /f "delims=" %%I in ('where python') do (
    set "PYTHON_EXE=%%I"
    goto :python_found
)
:python_found

"%PYTHON_EXE%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
    call :log [WARN] Python 3.10+ is required for best compatibility. Continuing anyway.
)

"%PYTHON_EXE%" -m pip --version >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
    call :log [WARN] pip is not available in the selected Python environment.
)

set "VENV_DIR=%REPO_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "RUN_PY=%PYTHON_EXE%"
if not exist "%VENV_PY%" (
    call :log Creating virtual environment...
    "%PYTHON_EXE%" -m venv "%VENV_DIR%" >>"%LOG_FILE%" 2>&1
    if errorlevel 1 (
        call :log [WARN] Failed to create .venv. Using system Python.
    )
)
if exist "%VENV_PY%" (
    set "RUN_PY=%VENV_PY%"
    call :log Installing dependencies...
    "%RUN_PY%" -m pip install -r "%REPO_ROOT%\requirements.txt" >>"%LOG_FILE%" 2>&1
    if errorlevel 1 (
        call :log [WARN] Dependency installation failed. Continuing with existing environment.
    )
) else (
    call :log [WARN] Skipping dependency install because .venv is unavailable.
)

call :log Running doctor checks (warning-first)...
"%RUN_PY%" -m mailbot_v26.doctor --config-dir "%CONFIG_DIR%" >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
    call :log [WARN] Doctor found issues. Startup continues in non-strict mode.
)

call :log Starting Letterbot stack via letterbot.bat...
REM "%RUN_PY%" -m mailbot_v26.doctor --config-dir "%REPO_ROOT%\mailbot_v26\config"
REM "%RUN_PY%" -m mailbot_v26.start --config-dir "%REPO_ROOT%\mailbot_v26\config"
call "%REPO_ROOT%\letterbot.bat" %* >>"%LOG_FILE%" 2>&1
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" (
    call :log Letterbot finished.
) else (
    call :log [ERROR] Letterbot terminated with errors.
)

call :finish %RUN_EXIT%
pause
exit /b %RUN_EXIT%

:log
set "MSG=%~1"
echo %MSG%
>>"%LOG_FILE%" echo [%DATE% %TIME%] %MSG%
exit /b 0

:finish
set "EXIT_CODE=%~1"
if "%EXIT_CODE%"=="0" (
    call :log [SUMMARY] OK
) else (
    call :log [SUMMARY] FAIL
    call :log [SUMMARY] See log: %LOG_FILE%
)
exit /b 0
