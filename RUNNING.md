# Running MailBot (one-click)

1. Double-click `run_mailbot.bat` in the repository root.

## Troubleshooting
- **Port in use**: stop the process using the port and retry.
- **DB locked**: close any other MailBot processes that might be using `data/mailbot.sqlite`.
- **Wrong password**: update `web_ui.password` in `config.yaml` and restart.

## Проверка перед релизом
- Двойной клик `ci_local.bat`.
- Скрипт сам прогонит compileall, pytest и one-folder build.
- Ожидаемый итог: `LOCAL CI OK`.


## LAN mode (safe)
- Set `web_ui.bind` to your LAN interface and keep `web_ui.allow_cidrs` limited to your subnet.
- Use `web_ui.password` with 10+ characters; defaults like `CHANGE_ME` are rejected when LAN is enabled.
- Set `web_ui.prod_server=true` to run Web UI on waitress; Flask built-in is for localhost development only.


## Support
- Настройте `support.methods` в `config.yaml` (карта/СБП/ЮMoney).
- После логина откройте `http://127.0.0.1:8787/support` (или ваш порт Web UI).
- Опциональный P.S. в TG дайджесте включается через `support.telegram` и ограничен `frequency_days`.
