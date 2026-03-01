@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
cd /d "%REPO_ROOT%"

set "LOG_DIR=%REPO_ROOT%logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\letterbot.log"

echo =============================================
echo   Letterbot Premium - Run
echo =============================================

where python >nul 2>&1
if ERRORLEVEL 1 (
    echo [ERROR] Python не найден в PATH.
    echo         Установите Python 3.10+ с python.org и запустите скрипт снова.
    pause
    exit /b 1
)

for /f "delims=" %%I in ('where python') do (
    set "PYTHON_EXE=%%I"
    goto :python_found
)
:python_found

set "VENV_DIR=%REPO_ROOT%.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "RUN_PY=%PYTHON_EXE%"

if not exist "%VENV_PY%" (
    echo [SETUP] Создание виртуального окружения...
    "%PYTHON_EXE%" -m venv "%VENV_DIR%" >>"%LOG_FILE%" 2>&1
    if ERRORLEVEL 1 (
        echo [WARN] Не удалось создать .venv. Используется системный Python.
    )
)
if exist "%VENV_PY%" (
    set "RUN_PY=%VENV_PY%"
)

echo [SETUP] Проверка зависимостей...
"%RUN_PY%" -m pip install -r "%REPO_ROOT%requirements.txt" --quiet >>"%LOG_FILE%" 2>&1
if ERRORLEVEL 1 (
    echo [WARN] Часть зависимостей не установилась. Продолжаем с текущим окружением.
)

if not exist "%REPO_ROOT%accounts.ini" (
    echo [SETUP] Создание шаблонов конфигурации...
    "%RUN_PY%" -m mailbot_v26 init-config --config-dir "%REPO_ROOT%" >nul 2>&1
    echo [SETUP] Заполните %REPO_ROOT%accounts.ini
    echo         Затем запустите letterbot.bat снова.
    if exist "%REPO_ROOT%accounts.ini" (
        start "" notepad "%REPO_ROOT%accounts.ini"
    )
    pause
    exit /b 2
)

"%RUN_PY%" -m mailbot_v26 config-ready --config-dir "%REPO_ROOT%" --verbose
if ERRORLEVEL 1 (
    echo.
    echo [SETUP] Конфигурация не заполнена. Откройте accounts.ini и заполните данные.
    echo         Путь: %REPO_ROOT%accounts.ini
    echo         Затем запустите letterbot.bat снова.
    pause
    exit /b 2
)

echo.
echo Running doctor checks (warning-first)...
"%RUN_PY%" -m mailbot_v26.doctor --config-dir "%REPO_ROOT%" >>"%LOG_FILE%" 2>&1
if ERRORLEVEL 1 (
    echo [WARN] Doctor нашёл проблемы. Запуск продолжается.
)

echo Running config validation (warning-first)...
"%RUN_PY%" -m mailbot_v26 validate-config --config-dir "%REPO_ROOT%" >>"%LOG_FILE%" 2>&1
if ERRORLEVEL 1 (
    echo [WARN] Конфигурация имеет предупреждения. Запуск продолжается.
)

echo.
echo Starting Letterbot...
"%RUN_PY%" -m mailbot_v26.tools.run_stack --config-dir "%REPO_ROOT%" --no-browser
set "RUN_EXIT=%ERRORLEVEL%"

if "%RUN_EXIT%"=="0" (
    echo [OK] Letterbot завершил работу.
) else (
    echo [ERROR] Letterbot завершился с ошибкой: %RUN_EXIT%
    echo         Лог: %LOG_FILE%
    pause
)
exit /b %RUN_EXIT%
