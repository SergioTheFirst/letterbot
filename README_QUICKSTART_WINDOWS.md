1) Распакуйте архив Letterbot в любую папку.
2) Дважды кликните `run.bat`.
3) Скопируйте шаблоны командой:
   - `copy mailbot_v26\config\settings.ini.example mailbot_v26\config\settings.ini`
   - `copy mailbot_v26\config\accounts.ini.example mailbot_v26\config\accounts.ini`
4) Откройте `settings.ini` и `accounts.ini`, заполните значения, сохраните.
5) Сохраните и закройте оба файла.
6) Снова запустите `run.bat`.

Для запуска из исходников репозитория используйте install_and_run.bat, затем run_mailbot.bat.

## CI artifact (Windows one-folder)
1) Откройте GitHub → Actions → последний run.
2) Скачайте artifact: Letterbot-windows-onefolder.
3) Распакуйте архив и запустите run_dist.bat или MailBot.exe.

## Доступ по локальной сети
1) Настройки Web задаются в settings.ini (секция [web]).
2) Перезапустите Letterbot.
3) Убедитесь, что Windows Firewall разрешает входящие подключения на выбранный порт.


## Если Windows показывает SmartScreen
- На первом запуске неподписанного `MailBot.exe` может появиться окно «Windows protected your PC».
- Нажмите `More info` → `Run anyway` (или «Подробнее» → «Выполнить в любом случае»).

Не открывайте `http://0.0.0.0:8787/` в браузере. Используйте `http://<IPv4_вашего_ПК>:8787/`.

Если LAN-страница не открывается, добавьте входящее правило Firewall: `netsh advfirewall firewall add rule name="Letterbot Web UI 8787" protocol=TCP dir=in localport=8787 action=allow`


Пример смены порта в settings.ini:
```ini
[web]
host = 127.0.0.1
port = 8790
```
