# Smoke Tests (Windows, <10 minutes)

Use this checklist after install.
Each scenario has one expected result and one direct fix.

## Scenario 1 — First run bootstrap `config.yaml` from example
- Preconditions: `config.yaml` is missing and `config.example.yaml` exists in the same folder as launcher.
- Action: Run `run_mailbot.bat` (source mode) or `run.bat` (dist mode).
- Expected result: Console shows `CONFIGURATION REQUIRED`, `config.yaml` is created, and Notepad opens.
- If FAIL, most likely cause: `config.example.yaml` is missing.
- Fix: Put `config.example.yaml` in the launcher folder and run again.

## Scenario 2 — Wrong YAML indentation or wrong root type
- Preconditions: `config.yaml` exists and contains malformed YAML.
- Action: Run `.venv\Scripts\python.exe -m mailbot_v26 validate-config`.
- Expected result: Exit code is non-zero and output explains YAML/config error.
- If FAIL, most likely cause: indentation or root mapping is invalid.
- Fix: Copy `config.example.yaml` to `config.yaml` and re-apply edits carefully.

Example minimal valid root:
```yaml
telegram:
  bot_token: "123456:abc"
  chat_id: 123456789
```

## Scenario 3 — Wrong Telegram token or chat id
- Preconditions: `telegram.bot_token` or `telegram.chat_id` is incorrect.
- Action: Run `.venv\Scripts\python.exe -m mailbot_v26 doctor`.
- Expected result: Doctor reports Telegram failure for auth/send.
- If FAIL, most likely cause: token typo or wrong chat id.
- Fix: Edit `config.yaml` with valid token and numeric chat id, then rerun doctor.

## Scenario 4 — Wrong IMAP credentials for one account, others OK
- Preconditions: Multiple accounts are configured and only one account has bad IMAP credentials.
- Action: Run `.venv\Scripts\python.exe -m mailbot_v26 doctor`.
- Expected result: Doctor IMAP section flags one account as FAIL and leaves valid accounts OK.
- If FAIL, most likely cause: wrong password or provider app-password mismatch for one account.
- Fix: Correct that account credentials in `config.yaml` and rerun doctor.

## Scenario 5 — LAN enabled but user opens `0.0.0.0` in browser
- Preconditions: `web_ui.enabled=true` and `web_ui.bind="0.0.0.0"`.
- Action: Open `http://0.0.0.0:<port>/` in browser.
- Expected result: It is not the address to browse; use LAN URL from doctor output.
- If FAIL, most likely cause: bind address is used instead of real PC IP.
- Fix: Run `.venv\Scripts\python.exe -m mailbot_v26 doctor --print-lan-url` and open `http://<PC IPv4>:<port>/`.

LAN snippet:
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

## Scenario 6 — LAN enabled but firewall blocks port
- Preconditions: MailBot is running and phone/second PC is in same LAN.
- Action: Open `http://<PC IPv4>:<port>/` from phone.
- Expected result: If blocked, localhost works but phone cannot connect.
- If FAIL, most likely cause: inbound firewall rule for this TCP port is missing.
- Fix: Run this command in `cmd` (replace `<port>`):

```bat
netsh advfirewall firewall add rule name="Letterbot Web UI <port>" protocol=TCP dir=in localport=<port> action=allow
```

## Scenario 7 — LAN enabled but CIDR allowlist blocks client
- Preconditions: `web_ui.allow_lan=true` and `web_ui.allow_cidrs` excludes client subnet.
- Action: Open UI from another device in LAN.
- Expected result: Request returns `Forbidden` (HTTP 403).
- If FAIL, most likely cause: client IP is outside configured CIDR list.
- Fix: Add correct subnet, for example `allow_cidrs: ["192.168.1.0/24"]`, then restart MailBot.

## Scenario 8 — Port already in use
- Preconditions: Another process already listens on configured `web_ui.port`.
- Action: Start MailBot.
- Expected result: Startup shows bind/port-in-use error and app does not start web server.
- If FAIL, most likely cause: port conflict.
- Fix: Change `web_ui.port` to a free port, save config, restart MailBot.

## Scenario 9 — SmartScreen shows “Windows protected your PC”
- Preconditions: First run of unsigned `MailBot.exe` on Windows.
- Action: Double-click `MailBot.exe`.
- Expected result: SmartScreen warning appears.
- If FAIL, most likely cause: unsigned binary reputation warning.
- Fix: Click `More info` → `Run anyway`.

## Scenario 10 — Diagnostics export redaction sanity and where to send
- Preconditions: Web UI login works and `/doctor` page is accessible.
- Action: Open Doctor page and export diagnostics zip.
- Expected result: Downloaded `diagnostics.zip` contains redacted config (`***REDACTED***`) and runtime/build/health files.
- If FAIL, most likely cause: support bundle was not exported from Doctor page.
- Fix: Export `diagnostics.zip` from `/doctor/export` and send it with this smoke checklist to support.
