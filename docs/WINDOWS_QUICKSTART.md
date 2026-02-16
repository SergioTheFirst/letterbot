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
  allow_cidrs:
    - "192.168.1.0/24"
  password: "use-10-plus-chars-here"
  prod_server: true
  require_strong_password_on_lan: true
```

Выбирайте CIDR только для своей подсети (обычно `192.168.x.0/24` или `10.x.x.0/24`), а не широкие диапазоны на весь офис/дом.

При `require_strong_password_on_lan: true` запуск блокируется для короткого пароля и дефолтов вроде `CHANGE_ME`/`password`, чтобы случайное пробрасывание порта не оставило UI с известным паролем.

`prod_server: true` включает waitress для LAN/production, а встроенный Flask-сервер оставляйте только для localhost и локальной отладки.
