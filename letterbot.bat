@echo off
setlocal EnableExtensions
chcp 65001 >nul
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
set "CONFIG_DIR=%REPO_ROOT%"
cd /d "%REPO_ROOT%"

set "RUNTIME_LOG_DIR=%REPO_ROOT%\runtime\logs"
if not exist "%RUNTIME_LOG_DIR%" mkdir "%RUNTIME_LOG_DIR%"
set "BOOT_LOG=%RUNTIME_LOG_DIR%\launcher.log"

where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python не найден в PATH. Установите Python 3.10+ и повторите запуск.
    exit /b 1
)

for /f "delims=" %%I in ('where python') do (
    set "PYTHON_EXE=%%I"
    goto :python_found
)
:python_found

set "VENV_DIR=%REPO_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "RUN_PY=%PYTHON_EXE%"

if not exist "%VENV_PY%" (
    echo [SETUP] Создание .venv...
    "%PYTHON_EXE%" -m venv "%VENV_DIR%" >>"%BOOT_LOG%" 2>&1
)
if exist "%VENV_PY%" set "RUN_PY=%VENV_PY%"

echo [SETUP] Установка зависимостей...
"%RUN_PY%" -m pip install -r "%REPO_ROOT%\requirements.txt" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 (
    echo [ERROR] Не удалось установить зависимости. См. %BOOT_LOG%
    exit /b 1
)

if not exist "%CONFIG_DIR%\settings.ini" goto :bootstrap_config
if not exist "%CONFIG_DIR%\accounts.ini" goto :bootstrap_config
goto :preflight

:bootstrap_config
echo [SETUP] Инициализация settings.ini и accounts.ini...
"%RUN_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >>"%BOOT_LOG%" 2>&1
if exist "%CONFIG_DIR%\accounts.ini" start "" notepad "%CONFIG_DIR%\accounts.ini"
echo [SETUP] Заполните accounts.ini и перезапустите letterbot.bat
exit /b 2

:preflight
echo [CHECK] config-ready
"%RUN_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
if errorlevel 1 (
    echo [ERROR] Конфигурация не готова. Заполните accounts.ini и повторите запуск.
    exit /b 2
)

echo [CHECK] doctor
"%RUN_PY%" -m mailbot_v26.doctor --config-dir "%CONFIG_DIR%"
if errorlevel 1 (
    echo [WARN] Doctor вернул предупреждения. Продолжаем запуск.
)

echo [CHECK] validate-config
"%RUN_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if errorlevel 1 (
    echo [WARN] validate-config вернул предупреждения. Продолжаем запуск.
)

echo [RUN] Запуск worker + web...
"%RUN_PY%" -m mailbot_v26.tools.run_stack --config-dir "%CONFIG_DIR%" --no-browser
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" exit /b 0

echo [ERROR] run_stack завершился с кодом %RUN_EXIT%
echo [ERROR] Логи процессов: %RUNTIME_LOG_DIR%
for %%F in ("%RUNTIME_LOG_DIR%\worker_*.log") do set "WORKER_LOG=%%~fF"
for %%F in ("%RUNTIME_LOG_DIR%\web_*.log") do set "WEB_LOG=%%~fF"
if defined WORKER_LOG (
    echo -------- worker tail --------
    powershell -NoProfile -Command "Get-Content -Path '%WORKER_LOG%' -Tail 30"
)
if defined WEB_LOG (
    echo -------- web tail --------
    powershell -NoProfile -Command "Get-Content -Path '%WEB_LOG%' -Tail 30"
)
exit /b %RUN_EXIT%
