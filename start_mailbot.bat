@echo off
setlocal

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

call "%REPO_ROOT%run_mailbot.bat"

endlocal
