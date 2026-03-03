# Troubleshooting (Windows)

Use one block per incident.
Each block is one symptom, one cause, one fix.

## 1) `CONFIGURATION REQUIRED` appears and app exits
- Symptom: Launcher prints `CONFIGURATION REQUIRED` and stops.
- Cause: `settings.ini` and `accounts.ini` were just created from examples and need user values.
- Fix: Fill required values in `settings.ini` and `accounts.ini`, save, then run launcher again.

## 2) `settings.ini.example` or `accounts.ini.example` not found
- Symptom: Launcher says that one of the required example files is missing.
- Cause: Template config file is missing in current folder/release bundle.
- Fix: Restore `mailbot_v26/config/settings.ini.example` and `mailbot_v26/config/accounts.ini.example`, then rerun launcher.

## 3) `validate-config` fails for INI configuration
- Symptom: `.venv\Scripts\python.exe -m mailbot_v26 validate-config` exits non-zero.
- Cause: Invalid value or syntax in `settings.ini` (`[general]` / `[web_ui]`) or `accounts.ini` account section.
- Fix: Restore from `settings.ini.example` / `accounts.ini.example`, then re-apply values carefully and re-run validation.

## 4) Telegram doctor check fails
- Symptom: Doctor output marks Telegram as FAIL.
- Cause: Invalid bot token or wrong chat id.
- Fix: Set valid `telegram_bot_token` and `telegram_chat_id` in `accounts.ini` (target account section).

## 5) One IMAP account fails, others pass
- Symptom: Doctor IMAP check fails only for one account.
- Cause: Wrong credentials or provider app-password policy for that account.
- Fix: Update that account credentials in `accounts.ini` and rerun doctor.

## 6) Opens on localhost but cannot open from phone
- Symptom: `http://127.0.0.1:<port>/` works, phone cannot connect.
- Cause: LAN bind/firewall path is incomplete.
- Fix: Set `bind = 0.0.0.0` and `allow_lan = true` in `settings.ini` (section `[web_ui]`), then add firewall rule for the port.

## 7) Browser opened `http://0.0.0.0:<port>/` and page does not work
- Symptom: User tries `0.0.0.0` URL directly.
- Cause: `0.0.0.0` is a listen address, not a client destination.
- Fix: Open `http://<PC IPv4>:<port>/` from `doctor --print-lan-url` output.

## 8) `Forbidden` from LAN client
- Symptom: UI returns `Forbidden` (HTTP 403) from another LAN device.
- Cause: Client IP is not included in `allow_cidrs` in `settings.ini` (section `[web_ui]`).
- Fix: Add correct client subnet CIDR and restart MailBot.

## 9) `Address already in use` or port bind failure
- Symptom: Startup reports port bind conflict.
- Cause: Another process already uses `port` in `settings.ini` (section `[web_ui]`).
- Fix: Change `port` to an unused value and restart.

## 10) `waitress` missing while `prod_server=true`
- Symptom: Startup fails with message about missing waitress.
- Cause: Production web server is requested but dependency is unavailable.
- Fix: Install runtime requirements in `.venv` and restart.

## 11) Dist run fails with missing `Letterbot.exe`
- Symptom: `run_dist.bat` or `run_dist.bat` says `Letterbot.exe not found`.
- Cause: Dist folder is incomplete or wrong launch directory is used.
- Fix: Start from `dist\Letterbot` folder that contains `Letterbot.exe`.

## 12) SmartScreen blocks first run
- Symptom: Windows shows `Windows protected your PC`.
- Cause: Unsigned executable reputation warning.
- Fix: Click `More info` → `Run anyway`.

## 13) Doctor export returns `Too many exports. Retry in ...`
- Symptom: `/doctor/export` responds with cooldown message.
- Cause: Export cooldown is active.
- Fix: Wait indicated seconds and retry once.

## 14) `manifest MODIFIED` shown in Doctor
- Symptom: Doctor page shows manifest status `MODIFIED`.
- Cause: Dist files differ from manifest baseline.
- Fix: Rebuild/re-extract clean one-folder dist and avoid manual binary edits.

## 15) Support asks for diagnostics but user sent screenshots only
- Symptom: Triage is blocked due to missing technical bundle.
- Cause: `diagnostics.zip` was not exported.
- Fix: Open `/doctor`, export `diagnostics.zip`, attach it with smoke checklist results.

---

## Устаревшие форматы конфигурации (legacy)

> **Только для существующих пользователей** с `config.yaml`, `config.ini`, или `keys.ini`.
> Новая установка: используйте `settings.ini` + `accounts.ini` (см. README.md).

### config.yaml (legacy YAML-режим)
- Поддерживается для обратной совместимости, но не является рекомендуемым форматом.
- Если используете — не переходите на новый формат посреди работы без резервной копии.
- При ошибке `Ошибка в config.yaml: web_ui.bind должен быть строкой` — добавьте
  `bind: "127.0.0.1"` и `port: 8787` в секцию `web_ui`.

### config.ini + keys.ini (legacy INI-режим)
- Устаревший двухфайловый формат (не `settings.ini`).
- Если до сих пор работает — оставьте как есть. Для новой установки не используйте.
- Для миграции: скопируйте значения из `config.ini`/`keys.ini` в соответствующие
  секции `settings.ini`, значения аккаунтов — в `accounts.ini`.
