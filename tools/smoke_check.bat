@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0.."
set "ROOT=%CD%"

set "STAMP="
for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "STAMP=%%i"
if not defined STAMP set "STAMP=unknown_timestamp"

set "ARTIFACT_DIR=%ROOT%\smoke_artifacts\%STAMP%"
mkdir "%ARTIFACT_DIR%" >nul 2>nul

set "SUMMARY_FILE=%ARTIFACT_DIR%\run_summary.txt"
set "EXIT_CODE=0"
set "MODE=unknown"

set "VENV_PY=%ROOT%\.venv\Scripts\python.exe"
set "DIST_EXE=%ROOT%\dist\MailBot\MailBot.exe"

set "WEB_PORT=8787"
if exist "%ROOT%\config.yaml" (
    for /f "tokens=2 delims=:" %%p in ('findstr /B /C:"  port:" "%ROOT%\config.yaml"') do (
        set "PORT_RAW=%%p"
    )
)
if defined PORT_RAW (
    set "PORT_RAW=%PORT_RAW: =%"
    if not "%PORT_RAW%"=="" set "WEB_PORT=%PORT_RAW%"
)

(
    echo smoke_check started=%DATE% %TIME%
    echo root=%ROOT%
) > "%SUMMARY_FILE%"

(
    echo os_version:
    ver
    echo.
    echo hostname:
    hostname
    echo.
    if exist "%VENV_PY%" (
        echo python_source_mode:
        "%VENV_PY%" -c "import platform,sys; print(platform.platform()); print(sys.version)"
    ) else (
        echo python_source_mode: not available
    )
) > "%ARTIFACT_DIR%\environment.txt" 2>&1

(
    echo Firewall rule hint for TCP port %WEB_PORT%:
    echo netsh advfirewall firewall add rule name="MailBot Web UI %WEB_PORT%" protocol=TCP dir=in localport=%WEB_PORT% action=allow
    echo.
    echo This script does not modify firewall automatically.
) > "%ARTIFACT_DIR%\firewall_hint.txt"

if exist "%VENV_PY%" (
    set "MODE=dev_source"
    echo mode=dev_source>> "%SUMMARY_FILE%"

    "%VENV_PY%" -m mailbot_v26 --version > "%ARTIFACT_DIR%\versions.txt" 2>&1
    if errorlevel 1 set "EXIT_CODE=2"

    "%VENV_PY%" -m mailbot_v26 validate-config > "%ARTIFACT_DIR%\config_check.txt" 2>&1
    if errorlevel 1 set "EXIT_CODE=2"

    "%VENV_PY%" -m mailbot_v26 doctor --print-lan-url > "%ARTIFACT_DIR%\doctor_print_lan_url.txt" 2>&1
    if errorlevel 1 set "EXIT_CODE=2"

    goto :write_summary
)

if exist "%DIST_EXE%" (
    set "MODE=dist_only"
    set "EXIT_CODE=2"
    echo mode=dist_only>> "%SUMMARY_FILE%"

    (
        echo Dist mode detected.
        echo MailBot.exe found at: %DIST_EXE%
        echo.
        echo Version check is unavailable without source python.
        echo Start MailBot.exe and verify UI login page opens.
        echo If SmartScreen appears: More info -^> Run anyway.
    ) > "%ARTIFACT_DIR%\versions.txt"

    (
        echo Dist mode detected.
        echo validate-config cannot run because .venv\Scripts\python.exe is missing.
        echo Manual check: ensure config.yaml exists and was edited after bootstrap.
    ) > "%ARTIFACT_DIR%\config_check.txt"

    (
        echo Dist mode detected.
        echo doctor --print-lan-url cannot run without source python command entrypoint.
        echo Manual LAN check:
        echo 1^) In config.yaml set web_ui.bind="0.0.0.0" and allow_lan=true.
        echo 2^) Find PC IPv4 with ipconfig.
        echo 3^) Open http://^<PC IPv4^>:%WEB_PORT%/
        echo 4^) If blocked, use firewall_hint.txt command.
    ) > "%ARTIFACT_DIR%\doctor_print_lan_url.txt"

    goto :write_summary
)

set "MODE=missing_prerequisites"
set "EXIT_CODE=2"
echo mode=missing_prerequisites>> "%SUMMARY_FILE%"

(
    echo Prerequisites missing.
    echo Expected one of:
    echo - %VENV_PY%
    echo - %DIST_EXE%
    echo.
    echo Run install_and_run.bat for source mode or build/extract dist\MailBot for dist mode.
) > "%ARTIFACT_DIR%\versions.txt"

(
    echo validate-config not runnable.
    echo Missing both source python and dist package context.
) > "%ARTIFACT_DIR%\config_check.txt"

(
    echo doctor --print-lan-url not runnable.
    echo Missing both source python and dist package context.
) > "%ARTIFACT_DIR%\doctor_print_lan_url.txt"

:write_summary
if "%EXIT_CODE%"=="0" (
    echo result=OK>> "%SUMMARY_FILE%"
) else (
    echo result=FAIL>> "%SUMMARY_FILE%"
)
echo exit_code=%EXIT_CODE%>> "%SUMMARY_FILE%"
echo artifact_dir=%ARTIFACT_DIR%>> "%SUMMARY_FILE%"

echo Smoke artifact: %ARTIFACT_DIR%
exit /b %EXIT_CODE%
