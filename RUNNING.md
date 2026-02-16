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
```yaml
web_ui:
  bind: "0.0.0.0"
  allow_lan: true
  allow_cidrs: ["192.168.0.0/16"]
  prod_server: true
```

- Narrow `allow_cidrs` to your home subnet when possible.
- Find your PC IPv4 with `ipconfig`.
- Open from phone/other PC: `http://<PC IPv4>:<port>/`.
- Do not open `http://0.0.0.0:<port>/` in browser.
- If needed, allow inbound TCP port in firewall:
  `netsh advfirewall firewall add rule name="MailBot Web UI <port>" protocol=TCP dir=in localport=<port> action=allow`
- Windows SmartScreen on first run may show “Windows protected your PC” for unsigned builds; click `More info` -> `Run anyway`.
- Keep `web_ui.password` strong (10+ chars) and keep `web_ui.prod_server=true` for LAN.


## Support
- Настройте `support.methods` в `config.yaml` (карта/СБП/ЮMoney).
- После логина откройте `http://127.0.0.1:8787/support` (или ваш порт Web UI).
- Опциональный P.S. в TG дайджесте включается через `support.telegram` и ограничен `frequency_days`.
