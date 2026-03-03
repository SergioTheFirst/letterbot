@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"
echo [DEPRECATED] update.bat -> update_and_run.bat
echo [DEPRECATED] Use update_and_run.bat instead.
echo.
call "%REPO_ROOT%update_and_run.bat" %*
exit /b %errorlevel%
