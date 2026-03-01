@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

REM DEPRECATED wrapper. Canonical script: letterbot.bat
REM Running doctor checks ^(warning-first^)...
REM [WARN] Doctor found issues. Startup continues in non-strict mode.
REM "%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%REPO_ROOT%mailbot_v26\config" --verbose
REM :CONFIG_READY_LOOP
REM if !CONFIG_READY_ATTEMPTS! GTR 20 (
REM   echo [ERROR] Конфигурация всё ещё не готова после 20 попыток. Бот не запущен.
REM   exit /b 2
REM )
REM echo [WARN] Попытка !CONFIG_READY_ATTEMPTS! из 20: обязательные поля ещё не заполнены.
REM "%VENV_PY%" -m mailbot_v26 config-ready --config-dir "%REPO_ROOT%mailbot_v26\config" --verbose
REM print(str(web.host)+' '+str(web.port))

echo [DEPRECATED] run_mailbot.bat -> letterbot.bat
call "%REPO_ROOT%letterbot.bat"
exit /b %ERRORLEVEL%
