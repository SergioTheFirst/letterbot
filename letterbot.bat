@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>&1
set "PYTHONUTF8=1"

set "REPO_ROOT=%~dp0"
if "%REPO_ROOT:~-1%"=="\" set "REPO_ROOT=%REPO_ROOT:~0,-1%"
cd /d "%REPO_ROOT%"

set "RUNTIME_LOG_DIR=%REPO_ROOT%\runtime\logs"
if not exist "%RUNTIME_LOG_DIR%" mkdir "%RUNTIME_LOG_DIR%"
set "BOOT_LOG=%RUNTIME_LOG_DIR%\launcher.log"
> "%BOOT_LOG%" (
    echo [launcher] started %DATE% %TIME%
    echo [launcher] repo_root=%REPO_ROOT%
)

set "PYTHON_EXE="
set "PYTHON_VERSION=unknown"
set "VENV_DIR=%REPO_ROOT%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
set "RUN_PY="
set "RUN_EXIT=0"
set "WORKER_LOG="
set "WEB_LOG="
set "WEB_URL=http://127.0.0.1:8787"

where python >nul 2>&1
if errorlevel 1 goto :error_python_missing

for /f "delims=" %%I in ('where python') do (
    set "PYTHON_EXE=%%I"
    goto :python_found
)
goto :error_python_missing

:python_found
for /f "delims=" %%V in ('"%PYTHON_EXE%" -c "import sys; print('%d.%d' %% sys.version_info[:2])" 2^>nul') do (
    set "PYTHON_VERSION=%%V"
)
"%PYTHON_EXE%" -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3, 10) else 1)" >nul 2>&1
if errorlevel 1 goto :error_python_version

if not exist "%VENV_PY%" (
    echo [SETUP] Создание .venv...
    "%PYTHON_EXE%" -m venv "%VENV_DIR%" >>"%BOOT_LOG%" 2>&1
    if errorlevel 1 goto :error_venv_create
)

if not exist "%VENV_PY%" goto :error_venv_create
set "RUN_PY=%VENV_PY%"

echo [SETUP] Установка зависимостей...
"%RUN_PY%" -m pip install -r "%REPO_ROOT%\requirements.txt" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 goto :error_dependency_install

if not exist "%REPO_ROOT%\settings.ini" goto :bootstrap_config
if not exist "%REPO_ROOT%\accounts.ini" goto :bootstrap_config

:preflight
echo [CHECK] config-ready
"%RUN_PY%" -m mailbot_v26 config-ready --config-dir "%CONFIG_DIR%" --verbose
if errorlevel 1 goto :error_config_not_ready

echo [CHECK] doctor
"%RUN_PY%" -m mailbot_v26 doctor --config-dir "%CONFIG_DIR%"
if errorlevel 1 (
    echo [WARN] Doctor вернул предупреждения. Продолжаем запуск.
)

echo [CHECK] validate-config
"%RUN_PY%" -m mailbot_v26 validate-config --config-dir "%CONFIG_DIR%"
if errorlevel 1 (
    echo [WARN] validate-config вернул предупреждения. Продолжаем запуск.
)

echo.
echo ==========================================
echo  Letterbot запущен!
echo ==========================================
echo  Web интерфейс: %WEB_URL%
echo  Остановить:    Ctrl+C
echo ==========================================
echo.
"%RUN_PY%" -m mailbot_v26.tools.run_stack --config-dir "%CONFIG_DIR%" --no-browser
set "RUN_EXIT=%ERRORLEVEL%"
if "%RUN_EXIT%"=="0" exit /b 0
goto :error_run_stack

:bootstrap_config
echo.
echo ==========================================
echo  Первый запуск — нужно заполнить настройки
echo ==========================================
echo.
echo  Сейчас откроется файл accounts.ini в Блокноте.
echo  Заполните данные почты и Telegram-бота.
echo.
echo  Замените CHANGE_ME в accounts.ini на реальные значения.
echo  После этого сохраните файл и снова запустите letterbot.bat.
echo.
"%RUN_PY%" -m mailbot_v26 init-config --config-dir "%CONFIG_DIR%" >>"%BOOT_LOG%" 2>&1
if errorlevel 1 goto :error_bootstrap_failed
if exist "%REPO_ROOT%\accounts.ini" (
    start "" notepad "%REPO_ROOT%\accounts.ini"
) else (
    goto :error_bootstrap_failed
)
echo  Нажмите любую клавишу чтобы закрыть это окно...
pause >nul
exit /b 2

:error_python_missing
echo.
echo ==========================================
echo  [ОШИБКА] Python не установлен
echo ==========================================
echo.
echo  Letterbot требует Python 3.10 или новее.
echo.
echo  Как установить:
echo  1. Откройте браузер
echo  2. Перейдите на https://www.python.org/downloads/
echo  3. Нажмите "Download Python 3.x.x"
echo  4. При установке отметьте "Add Python to PATH"
echo  5. После установки перезапустите letterbot.bat
echo.
echo  Лог: %BOOT_LOG%
echo.
pause
exit /b 1

:error_python_version
echo.
echo ==========================================
echo  [ОШИБКА] Версия Python слишком старая
echo ==========================================
echo.
echo  Найдена версия: %PYTHON_VERSION%
echo  Нужна версия Python 3.10 или новее.
echo.
echo  Установите новую версию с https://www.python.org/downloads/
echo  Затем снова запустите letterbot.bat
echo.
echo  Лог: %BOOT_LOG%
echo.
pause
exit /b 1

:error_venv_create
echo.
echo ==========================================
echo  [ОШИБКА] Не удалось создать .venv
echo ==========================================
echo.
echo  Что нужно сделать:
echo  1. Проверьте, что Python 3.10+ установлен корректно
echo  2. Убедитесь, что в папке проекта есть права на запись
echo.
echo  Детали ошибки:
call :print_boot_log_tail
echo.
pause
exit /b 1

:error_dependency_install
echo.
echo ==========================================
echo  [ОШИБКА] Не удалось установить зависимости
echo ==========================================
echo.
echo  Возможные причины:
echo  1. Нет подключения к интернету
echo  2. Антивирус блокирует pip
echo  3. Нужна версия Python 3.10+
echo.
echo  Детали ошибки:
call :print_boot_log_tail
echo.
pause
exit /b 1

:error_bootstrap_failed
echo.
echo ==========================================
echo  [ОШИБКА] Не удалось создать файлы настроек
echo ==========================================
echo.
echo  Что нужно сделать:
echo  1. Проверьте права на запись в папку проекта
echo  2. Повторно запустите letterbot.bat
echo.
echo  Лог: %BOOT_LOG%
echo.
call :print_boot_log_tail
echo.
pause
exit /b 2

:error_config_not_ready
echo.
echo ==========================================
echo  [ОШИБКА] Конфигурация не заполнена
echo ==========================================
echo.
echo  Откройте accounts.ini и замените все CHANGE_ME.
echo  Файл: %REPO_ROOT%\accounts.ini
echo.
echo  Открыть файл сейчас? Нажмите любую клавишу...
pause >nul
if exist "%REPO_ROOT%\accounts.ini" start "" notepad "%REPO_ROOT%\accounts.ini"
echo.
echo  После заполнения сохраните файл и запустите letterbot.bat снова.
echo.
pause
exit /b 2

:error_run_stack
echo.
echo ==========================================
echo  [ОШИБКА] Letterbot остановился неожиданно (код: %RUN_EXIT%)
echo ==========================================
echo.
echo  Последние строки лога запуска:
call :print_boot_log_tail
echo.
call :resolve_runtime_logs
if defined WORKER_LOG (
    echo  Последние строки worker-лога:
    powershell -NoProfile -Command "Get-Content -Path '%WORKER_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
    echo.
)
if defined WEB_LOG (
    echo  Последние строки web-лога:
    powershell -NoProfile -Command "Get-Content -Path '%WEB_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
    echo.
)
echo  Нажмите любую клавишу для выхода...
pause >nul
exit /b %RUN_EXIT%

:print_boot_log_tail
if exist "%BOOT_LOG%" (
    powershell -NoProfile -Command "Get-Content -Path '%BOOT_LOG%' -Tail 20 -ErrorAction SilentlyContinue"
) else (
    echo  Лог пока не создан.
)
exit /b 0

:resolve_runtime_logs
set "WORKER_LOG="
set "WEB_LOG="
for /f "delims=" %%F in ('dir /b /a-d /o-d "%RUNTIME_LOG_DIR%\worker_*.log" 2^>nul') do (
    if not defined WORKER_LOG set "WORKER_LOG=%RUNTIME_LOG_DIR%\%%F"
)
for /f "delims=" %%F in ('dir /b /a-d /o-d "%RUNTIME_LOG_DIR%\web_*.log" 2^>nul') do (
    if not defined WEB_LOG set "WEB_LOG=%RUNTIME_LOG_DIR%\%%F"
)
exit /b 0
