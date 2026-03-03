@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

echo [DEPRECATED] start_mailbot.bat -> letterbot.bat
echo [DEPRECATED] Use letterbot.bat instead.
echo.
call "%~dp0letterbot.bat" %*
exit /b %errorlevel%
