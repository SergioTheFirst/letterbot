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


## LAN mode (safe)
```yaml
web_ui:
  enabled: true
  bind: "0.0.0.0"
  port: 8787
  allow_lan: true
  allow_cidrs: ["192.168.0.0/16"]
  password: "use-10-plus-chars-here"
  prod_server: true
  require_strong_password_on_lan: true
```

Сузьте `allow_cidrs` до вашей подсети, если знаете точный диапазон (например `192.168.1.0/24`).

Найдите IP компьютера: откройте `cmd` и выполните `ipconfig`, затем возьмите `IPv4 Address`.

Откройте с телефона/другого ПК: `http://<IPv4_вашего_ПК>:8787/`.

Не открывайте `http://0.0.0.0:8787/` в браузере: это адрес прослушивания, а не адрес для подключения.

Если страница не открывается, добавьте правило Windows Firewall (PowerShell/cmd):

`netsh advfirewall firewall add rule name="MailBot Web UI 8787" protocol=TCP dir=in localport=8787 action=allow`

`prod_server: true` включает waitress для LAN/production, а встроенный Flask-сервер оставляйте только для localhost и локальной отладки.


## Windows SmartScreen (первый запуск)
Если при запуске `MailBot.exe` видно «Windows protected your PC», это стандартное предупреждение для неподписанных файлов. Нажмите `More info` → `Run anyway` (или «Подробнее» → «Выполнить в любом случае»).
