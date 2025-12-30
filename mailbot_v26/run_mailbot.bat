@echo off
setlocal
chcp 65001 >nul
title MailBot Premium - Deprecated Runner

echo ===========================================
echo   DEPRECATED: use root run_mailbot.bat
echo ===========================================

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
call "%REPO_ROOT%\run_mailbot.bat"
set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
