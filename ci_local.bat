@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

cd /d "%~dp0"
set "VENV_PY=.venv\Scripts\python.exe"
set "CI_FAILED=0"

if not exist "%VENV_PY%" (
    echo Запустите install_and_run.bat
    exit /b 1
)

echo [A] Installing runtime requirements...
"%VENV_PY%" -m pip install -r requirements.txt || set "CI_FAILED=1"
if "%CI_FAILED%"=="1" goto :ci_failed

echo [B] Installing build requirements...
"%VENV_PY%" -m pip install -r requirements-build.txt || set "CI_FAILED=1"
if "%CI_FAILED%"=="1" goto :ci_failed

echo [C] Compiling Python sources...
"%VENV_PY%" -m compileall mailbot_v26 || set "CI_FAILED=1"
if "%CI_FAILED%"=="1" goto :ci_failed

echo [D] Running tests...
"%VENV_PY%" -m pytest -q || set "CI_FAILED=1"
if "%CI_FAILED%"=="1" goto :ci_failed

echo [E] Building one-folder distribution...
cmd /c build_windows_onefolder.bat || set "CI_FAILED=1"
if "%CI_FAILED%"=="1" goto :ci_failed

if not exist "dist\MailBot" (
    echo dist\MailBot not found after build
    set "CI_FAILED=1"
    goto :ci_failed
)

if not exist "dist\MailBot\manifest.sha256.json" (
    echo dist\MailBot\manifest.sha256.json not found
    set "CI_FAILED=1"
    goto :ci_failed
)

if not exist "dist\MailBot\config.example.yaml" (
    echo dist\MailBot\config.example.yaml not found
    set "CI_FAILED=1"
    goto :ci_failed
)

if not exist "dist\MailBot\README_QUICKSTART_WINDOWS.md" (
    echo dist\MailBot\README_QUICKSTART_WINDOWS.md not found
    set "CI_FAILED=1"
    goto :ci_failed
)

echo LOCAL CI OK
exit /b 0

:ci_failed
echo LOCAL CI FAILED
exit /b 1
