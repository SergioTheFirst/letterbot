@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
cd /d "%REPO_ROOT%" || goto :error_repo_root

set "CONFIG_DIR=%REPO_ROOT%"
set "DATA_DIR=%REPO_ROOT%\data"
set "LEGACY_LOG_DIR=%REPO_ROOT%\logs"
set "RUNTIME_LOG_DIR=%REPO_ROOT%\runtime\logs"
set "BOOT_LOG=%RUNTIME_LOG_DIR%\launcher.log"
set "VENV_DIR=%REPO_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "REQ_FILE=%REPO_ROOT%\requirements.txt"
set "REQ_STAMP=%VENV_DIR%\.deps_ready"
set "RUN_PY="
set "RUN_EXIT=0"
set "WORKER_LOG="
set "WEB_LOG="
set "WEB_URL=http://127.0.0.1:8787"
set "PYTHON_EXE="
set "PYTHON_VERSION="
set "PYTHON_COMMAND="
set "OLD_PYTHON_VERSION="
set "OLD_PYTHON_COMMAND="

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>&1
if not exist "%LEGACY_LOG_DIR%" mkdir "%LEGACY_LOG_DIR%" >nul 2>&1
if not exist "%RUNTIME_LOG_DIR%" mkdir "%RUNTIME_LOG_DIR%" >nul 2>&1

> "%BOOT_LOG%" (
    echo [launcher] started %DATE% %TIME%
    echo [launcher] repo_root=%REPO_ROOT%
)

if exist "%VENV_PY%" (
    set "RUN_PY=%VENV_PY%"
    goto :venv_ready
)

call :find_python
if defined PYTHON_COMMAND goto :create_venv
if defined OLD_PYTHON_VERSION goto :error_python_version
goto :error_python_missing

:create_venv
echo [SETUP] Creating .venv...
call :run_python_command %PYTHON_COMMAND% -m venv "%VENV_DIR%" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 goto :error_venv_create
if not exist "%VENV_PY%" goto :error_venv_create
set "RUN_PY=%VENV_PY%"

:venv_ready
if not exist "%REQ_FILE%" goto :error_requirements_missing
call :install_requirements_if_needed
if errorlevel 1 goto :error_dependency_install

if not exist "%CONFIG_DIR%\settings.ini" goto :bootstrap_config
if not exist "%CONFIG_DIR%\accounts.ini" goto :bootstrap_config

:preflight
echo [CHECK] config-ready
"%RUN_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
if errorlevel 1 goto :error_config_not_ready

echo [CHECK] doctor
"%RUN_PY%" -m mailbot_v26 doctor --config-dir "%CONFIG_DIR%"
if errorlevel 1 echo [WARN] doctor reported warnings. Continuing.

echo [CHECK] validate-config
"%RUN_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if errorlevel 1 echo [WARN] validate-config reported warnings. Continuing.

echo.
echo ==========================================
echo  LetterBot.ru is running.
echo ==========================================
echo  Web UI:  %WEB_URL%
echo  Stop:    Ctrl+C
echo ==========================================
echo.
"%RUN_PY%" -m mailbot_v26.tools.run_stack --config-dir "%CONFIG_DIR%" --no-browser
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" exit /b 0
goto :error_run_stack

:bootstrap_config
echo.
echo ==========================================
echo  First run setup
echo ==========================================
echo.
echo  LetterBot.ru created local config files in this folder.
echo  accounts.ini will open in Notepad now.
echo  Fill in your mail login, password, host, and Telegram values.
echo  Save the file, close Notepad, and run letterbot.bat again.
echo.
"%RUN_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 goto :error_bootstrap_failed
call :open_accounts_ini
if errorlevel 1 goto :error_bootstrap_failed
echo.
echo  Press any key to close this window.
call :pause_if_needed
exit /b 2

:error_repo_root
echo.
echo ==========================================
echo  [ERROR] Could not switch to the LetterBot.ru folder
echo ==========================================
echo.
echo  Move the extracted LetterBot.ru folder to a local drive and run letterbot.bat again.
echo.
call :pause_if_needed
exit /b 1

:error_python_missing
echo.
echo ==========================================
echo  [ERROR] Python was not found
echo ==========================================
echo.
echo  LetterBot.ru needs Python 3.10 or newer.
echo  Install it from https://www.python.org/downloads/
echo  During setup, enable: Add Python to PATH
echo  Then run letterbot.bat again.
echo.
echo  Log: %BOOT_LOG%
echo.
call :pause_if_needed
exit /b 1

:error_python_version
echo.
echo ==========================================
echo  [ERROR] Python is too old
echo ==========================================
echo.
echo  Found: %OLD_PYTHON_VERSION%
echo  Needed: Python 3.10 or newer
echo  Install a newer version from https://www.python.org/downloads/
echo  Then run letterbot.bat again.
echo.
call :pause_if_needed
exit /b 1

:error_venv_create
echo.
echo ==========================================
echo  [ERROR] Could not create .venv
echo ==========================================
echo.
echo  Check that Python 3.10+ is installed correctly and that this folder is writable.
echo  Log: %BOOT_LOG%
echo.
call :print_boot_log_tail
echo.
call :pause_if_needed
exit /b 1

:error_requirements_missing
echo.
echo ==========================================
echo  [ERROR] requirements.txt is missing
echo ==========================================
echo.
echo  This archive looks incomplete. Download LetterBot.ru again from GitHub and extract it fully.
echo.
call :pause_if_needed
exit /b 1

:error_dependency_install
echo.
echo ==========================================
echo  [ERROR] Could not install dependencies
echo ==========================================
echo.
echo  Possible causes:
echo  1. No internet connection
echo  2. Antivirus blocked pip
echo  3. Python 3.10+ is not installed correctly
echo.
echo  Last log lines:
call :print_boot_log_tail
echo.
call :pause_if_needed
exit /b 1

:error_bootstrap_failed
echo.
echo ==========================================
echo  [ERROR] Could not create local config files
echo ==========================================
echo.
echo  Check that this folder is writable, then run letterbot.bat again.
echo  Log: %BOOT_LOG%
echo.
call :print_boot_log_tail
echo.
call :pause_if_needed
exit /b 2

:error_config_not_ready
echo.
echo ==========================================
echo  [ERROR] Config is not ready
echo ==========================================
echo.
echo  Open accounts.ini and replace every CHANGE_ME value.
echo  File: %CONFIG_DIR%\accounts.ini
echo.
call :open_accounts_ini
echo.
echo  Save the file, close Notepad, and run letterbot.bat again.
echo.
call :pause_if_needed
exit /b 2

:error_run_stack
echo.
echo ==========================================
echo  [ERROR] LetterBot.ru stopped unexpectedly
echo ==========================================
echo.
echo  Exit code: %RUN_EXIT%
echo  Last launcher log lines:
call :print_boot_log_tail
echo.
call :resolve_runtime_logs
if defined WORKER_LOG (
    echo  Last worker log lines:
    powershell -NoProfile -Command "Get-Content -Path '%WORKER_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
    echo.
)
if defined WEB_LOG (
    echo  Last web log lines:
    powershell -NoProfile -Command "Get-Content -Path '%WEB_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
    echo.
)
call :pause_if_needed
exit /b %RUN_EXIT%

:find_python
call :probe_python py -3.10
call :probe_python py -3
call :probe_python py
call :probe_python python
exit /b 0

:probe_python
if defined PYTHON_EXE exit /b 0
set "PROBE_EXE="
set "PROBE_VER="
set "PROBE_NUM="
set "PROBE_CMD=%*"
for /f "usebackq tokens=1,2,3 delims=|" %%A in (`%* -c "import sys; print(sys.executable + '^|' + str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '^|' + str(sys.version_info[0] * 100 + sys.version_info[1]))" 2^>nul`) do (
    set "PROBE_EXE=%%~A"
    set "PROBE_VER=%%~B"
    set "PROBE_NUM=%%~C"
)
if not defined PROBE_EXE exit /b 0
if %PROBE_NUM% GEQ 310 goto :probe_python_accept
if not defined OLD_PYTHON_VERSION set "OLD_PYTHON_VERSION=%PROBE_VER%"
if not defined OLD_PYTHON_COMMAND set "OLD_PYTHON_COMMAND=%PROBE_CMD%"
exit /b 0

:probe_python_accept
set "PYTHON_EXE=%PROBE_EXE%"
set "PYTHON_VERSION=%PROBE_VER%"
set "PYTHON_COMMAND=%PROBE_CMD%"
exit /b 0

:install_requirements_if_needed
if exist "%REQ_STAMP%" (
    fc /b "%REQ_FILE%" "%REQ_STAMP%" >nul
    if not errorlevel 1 (
        echo [SETUP] Dependencies are up to date.
        exit /b 0
    )
)
echo [SETUP] Installing dependencies...
"%RUN_PY%" -m pip install -r "%REQ_FILE%" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 exit /b 1
copy /Y "%REQ_FILE%" "%REQ_STAMP%" >nul
if errorlevel 1 exit /b 1
exit /b 0

:open_accounts_ini
if not exist "%CONFIG_DIR%\accounts.ini" exit /b 1
if /I "%LETTERBOT_SKIP_NOTEPAD%"=="1" (
    echo [INFO] Skipping Notepad because LETTERBOT_SKIP_NOTEPAD=1.
) else (
    start "" notepad "%CONFIG_DIR%\accounts.ini"
)
exit /b 0

:run_python_command
%*
exit /b %ERRORLEVEL%

:print_boot_log_tail
if exist "%BOOT_LOG%" (
    powershell -NoProfile -Command "Get-Content -Path '%BOOT_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
) else (
    echo  Launcher log was not created yet.
)
exit /b 0

:resolve_runtime_logs
set "WORKER_LOG="
set "WEB_LOG="
for /f "delims=" %%F in ('dir /b /a-d /o-d "%RUNTIME_LOG_DIR%\worker_*.log" 2^>nul') do (
    if not defined WORKER_LOG set "WORKER_LOG=%RUNTIME_LOG_DIR%\%%F"
)
for /f "delims=" %%F in ('dir /b /a-d /o-d "%RUNTIME_LOG_DIR%\web_*.log" 2^>nul') do (
    if not defined WEB_LOG set "WEB_LOG=%RUNTIME_LOG_DIR%\%%F"
)
exit /b 0

:pause_if_needed
if /I "%LETTERBOT_SKIP_PAUSE%"=="1" exit /b 0
echo.
pause
exit /b 0
