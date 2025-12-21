@echo off
chcp 65001 >nul
title MailBot Premium v26 - Launcher

echo ===========================================
echo      MAILBOT PREMIUM v26 - START
echo ===========================================

REM Переход в корень репозитория
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set REPO_ROOT=%%~fI
echo Переход в корень репозитория...
cd /d "%REPO_ROOT%"

echo Проверка Python...
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python not found!
    pause
    exit /B 1
)

echo Активация виртуального окружения...
IF EXIST mailbot_v26\venv\Scripts\activate (
    call mailbot_v26\venv\Scripts\activate
) ELSE IF EXIST venv\Scripts\activate (
    call venv\Scripts\activate
)

echo Запуск MailBot...
python -m mailbot_v26.start

echo.
echo ===========================================
echo            BOT FINISHED / STOPPED
echo ===========================================
pause
