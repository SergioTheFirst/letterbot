@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

REM DEPRECATED wrapper. Canonical script: letterbot.bat
REM "%VENV_PY%" -m mailbot_v26 migrate-config --config-dir "%~dp0mailbot_v26\config"
REM "%VENV_PY%" -m mailbot_v26.doctor --config-dir "%~dp0mailbot_v26\config"
REM "%VENV_PY%" -m mailbot_v26.start --config-dir "%~dp0mailbot_v26\config"

echo [DEPRECATED] install_and_run.bat -> letterbot.bat
call "%REPO_ROOT%letterbot.bat"
exit /b %ERRORLEVEL%
