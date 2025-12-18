@echo off
set REPO_URL=https://github.com/SergioTheFirst/mailpro.git
set LOCAL_DIR=C:\pro\mailbot

if exist "%LOCAL_DIR%" (
    rmdir /s /q "%LOCAL_DIR%"
)

git clone %REPO_URL% "%LOCAL_DIR%"
pause
