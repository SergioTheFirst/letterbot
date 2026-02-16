# Windows Quickstart
1. Установите Python 3.10+ и клонируйте репозиторий в `C:\pro\mailpro`.
2. Дважды кликните `install_and_run.bat` в корне, дождитесь установки зависимостей.
3. Для повторного запуска используйте `run_mailbot.bat` из того же каталога.
4. Для запуска тестов откройте `run_tests.bat` в корне.
5. Скрипты автоматически создают venv в корне и запускают `python -m mailbot_v26`.
6. Все установки выполняйте через `.venv\Scripts\python -m pip ...` (включая `install_and_run.bat` и `ci_local.bat`).

Опциональный офлайн-путь (если есть прокси/блокировки):
- На машине с доступом в интернет: `.venv\Scripts\python -m pip download -r requirements.txt -d wheelhouse`
- На целевой машине: `.venv\Scripts\python -m pip install --no-index --find-links wheelhouse -r requirements.txt`

> LAN-режим открывает доступ в вашей сети, используйте сильный пароль.
