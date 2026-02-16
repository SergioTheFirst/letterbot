# Troubleshooting (Windows)

Use one block per incident.
Each block is one symptom, one cause, one fix.

## 1) `CONFIGURATION REQUIRED` appears and app exits
- Symptom: Launcher prints `CONFIGURATION REQUIRED` and stops.
- Cause: `config.yaml` was just created from example and needs user values.
- Fix: Fill `config.yaml` placeholders, save, then run launcher again.

## 2) `config.example.yaml not found`
- Symptom: Launcher says `config.example.yaml not found`.
- Cause: Example config file is missing in current folder.
- Fix: Restore `config.example.yaml` into repo/dist root and rerun launcher.

## 3) `validate-config` fails with YAML error
- Symptom: `.venv\Scripts\python.exe -m mailbot_v26 validate-config` exits non-zero.
- Cause: YAML indentation or structure is invalid.
- Fix: Replace with clean `config.example.yaml`, then edit only required values.

## 4) Telegram doctor check fails
- Symptom: Doctor output marks Telegram as FAIL.
- Cause: Invalid bot token or wrong chat id.
- Fix: Set valid `telegram.bot_token` and `telegram.chat_id` in `config.yaml`.

## 5) One IMAP account fails, others pass
- Symptom: Doctor IMAP check fails only for one account.
- Cause: Wrong credentials or provider app-password policy for that account.
- Fix: Update that account credentials in `config.yaml` and rerun doctor.

## 6) Opens on localhost but cannot open from phone
- Symptom: `http://127.0.0.1:<port>/` works, phone cannot connect.
- Cause: LAN bind/firewall path is incomplete.
- Fix: Set `web_ui.bind: "0.0.0.0"`, confirm `allow_lan: true`, then add firewall rule for the port.

## 7) Browser opened `http://0.0.0.0:<port>/` and page does not work
- Symptom: User tries `0.0.0.0` URL directly.
- Cause: `0.0.0.0` is a listen address, not a client destination.
- Fix: Open `http://<PC IPv4>:<port>/` from `doctor --print-lan-url` output.

## 8) `Forbidden` from LAN client
- Symptom: UI returns `Forbidden` (HTTP 403) from another LAN device.
- Cause: Client IP is not included in `web_ui.allow_cidrs`.
- Fix: Add correct client subnet CIDR and restart MailBot.

## 9) `Address already in use` or port bind failure
- Symptom: Startup reports port bind conflict.
- Cause: Another process already uses `web_ui.port`.
- Fix: Change `web_ui.port` to an unused value and restart.

## 10) `waitress` missing while `prod_server=true`
- Symptom: Startup fails with message about missing waitress.
- Cause: Production web server is requested but dependency is unavailable.
- Fix: Install runtime requirements in `.venv` and restart.

## 11) Dist run fails with missing `MailBot.exe`
- Symptom: `run.bat` or `run_dist.bat` says `MailBot.exe not found`.
- Cause: Dist folder is incomplete or wrong launch directory is used.
- Fix: Start from `dist\MailBot` folder that contains `MailBot.exe`.

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
