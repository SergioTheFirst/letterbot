@echo off
set SCRIPT_DIR=%~dp0
set LOCAL_DIR=C:\pro\mailbot

cd /d "%LOCAL_DIR%" || exit /b 1

git rev-parse --is-inside-work-tree >nul 2>&1 || exit /b 1

git add -A
git commit -m "auto update from local"
git push origin main

pause
