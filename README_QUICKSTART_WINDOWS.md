1) Распакуйте архив Letterbot в любую папку.
2) Дважды кликните `run.bat`.
3) Launcher автоматически создаст:
   - `mailbot_v26\config\settings.ini` из `settings.ini.example`
   - `mailbot_v26\config\accounts.ini` из `accounts.ini.example`
4) Откройте `accounts.ini`, заполните обязательные поля IMAP:
   - `login, password, host, port, use_ssl`
5) Сохраните файл и закройте Блокнот.
6) Launcher повторно проверит `config-ready` и запустит Letterbot.

Для запуска из исходников репозитория используйте `install_and_run.bat`, затем `run_mailbot.bat`.

## CI artifact (Windows one-folder)
1) Откройте GitHub → Actions → последний run.
2) Скачайте artifact: `Letterbot-windows-onefolder`.
3) Распакуйте архив `Letterbot.zip` и запустите `run.bat` или `Letterbot.exe`.

## Доступ по локальной сети
1) Настройки Web задаются в `settings.ini` (секция `[web]`).
2) Перезапустите Letterbot.
3) Убедитесь, что Windows Firewall разрешает входящие подключения на выбранный порт.

## Если Windows показывает SmartScreen
- На первом запуске неподписанного `Letterbot.exe` может появиться окно «Windows protected your PC».
- Нажмите `More info` → `Run anyway` (или «Подробнее» → «Выполнить в любом случае»).

Не открывайте `http://0.0.0.0:8787/` в браузере. Используйте `http://<IPv4_вашего_ПК>:8787/`.

Если LAN-страница не открывается, добавьте входящее правило Firewall: `netsh advfirewall firewall add rule name="Letterbot Web UI 8787" protocol=TCP dir=in localport=8787 action=allow`

Пример смены порта в settings.ini:
```ini
[web]
host = 127.0.0.1
port = 8790
```
