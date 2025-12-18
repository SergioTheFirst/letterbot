@echo off
set SCRIPT_DIR=%~dp0
set LOCAL_DIR=C:\pro\mailbot
set REPO_URL=https://github.com/SergioTheFirst/mailpro.git

if exist "%LOCAL_DIR%" (
    rmdir /s /q "%LOCAL_DIR%"
)

git clone %REPO_URL% "%LOCAL_DIR%"
pause
