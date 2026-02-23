@echo off
setlocal enableextensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

call "%REPO_ROOT%run_mailbot.bat"
exit /b %ERRORLEVEL%
