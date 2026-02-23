1) Распакуйте архив Letterbot в любую папку.
2) Дважды кликните `run.bat`.
3) Скопируйте шаблоны командой:
   - `copy mailbot_v26\config\config.ini.example mailbot_v26\config\config.ini`
   - `copy config.yaml.example config.yaml`
4) Если `config.yaml` отсутствует, он создастся из `config.yaml.example` и откроется в Блокноте.
5) Заполните значения в кавычках (`null` без кавычек), сохраните и закройте файл.
6) Снова запустите `run.bat`.

Для запуска из исходников репозитория используйте install_and_run.bat, затем run_mailbot.bat.

## CI artifact (Windows one-folder)
1) Откройте GitHub → Actions → последний run.
2) Скачайте artifact: Letterbot-windows-onefolder.
3) Распакуйте архив и запустите run_dist.bat или MailBot.exe.

## Доступ по локальной сети
1) Откройте config.yaml и задайте:
   - web_ui.bind = "0.0.0.0"
   - web_ui.allow_lan = true
   - web_ui.allow_cidrs = ["192.168.0.0/16", "10.0.0.0/8", "172.16.0.0/12"]
   - web_ui.password = "СИЛЬНЫЙ_ПАРОЛЬ"
2) Перезапустите Letterbot.
3) Убедитесь, что Windows Firewall разрешает входящие подключения на выбранный порт.


## Если Windows показывает SmartScreen
- На первом запуске неподписанного `MailBot.exe` может появиться окно «Windows protected your PC».
- Нажмите `More info` → `Run anyway` (или «Подробнее» → «Выполнить в любом случае»).

Не открывайте `http://0.0.0.0:8787/` в браузере. Используйте `http://<IPv4_вашего_ПК>:8787/`.

Если LAN-страница не открывается, добавьте входящее правило Firewall: `netsh advfirewall firewall add rule name="Letterbot Web UI 8787" protocol=TCP dir=in localport=8787 action=allow`
