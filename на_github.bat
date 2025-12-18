@echo off
set LOCAL_DIR=C:\pro\mailbot

cd /d "%LOCAL_DIR%"

git add -A
git commit -m "auto update from local"
git push origin master

pause
