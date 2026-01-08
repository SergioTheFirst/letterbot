@echo off
setlocal
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

set "PYTHON_EXE=python"
if exist "%REPO_ROOT%.venv\Scripts\python.exe" (
    set "PYTHON_EXE=%REPO_ROOT%.venv\Scripts\python.exe"
)

%PYTHON_EXE% -m mailbot_v26.tools.run_stack all
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: MailBot stack terminated with errors.
)

echo =============================================
echo   DONE. Close this window or press a key.
echo =============================================
pause
endlocal
