# Production Gates (Windows one-folder zip)

## G1 — First-run succeeds

**Definition:**
- User can unpack one-folder build, create `config.yaml`, run `run.bat`, and reach steady state with mail polling + Telegram delivery + Web UI.

**Manual test on clean Windows machine:**
1. Unpack `MailBot.zip` to `C:\MailBot`.
2. Open `C:\MailBot\run.bat`.
3. When prompted, fill `config.yaml` with valid Telegram token/chat_id, one valid IMAP account, valid LLM provider token, and save.
4. Re-run `run.bat`.
5. Send a test email into configured mailbox.
6. Confirm one Telegram notification arrives.
7. Open `http://127.0.0.1:<web_ui.port>` and log in.

**Pass evidence (logs/messages):**
- `MAILBOT PREMIUM ... - STARTING` banner in console.
- `[OK] Loaded <N> accounts` in console.
- No `[ERROR] config...` line.
- Telegram receives one message per email.

**Fail evidence:**
- `ERROR: config validation failed.` in `run_mailbot.bat` launcher output.
- `[ERROR] config.yaml ...` from startup loader.
- No Telegram delivery and `telegram_delivery_failed` in logs.

**User-visible result:**
- User sees first Telegram notification and can open Web UI without editing code.

---

## G2 — Config errors are actionable

**Definition:**
- Invalid config files must fail fast with explicit field-level error message in launcher/startup output.

**Manual test on clean Windows machine:**
1. Copy `config.example.yaml` to `config.yaml`.
2. Intentionally break one field per run:
   - remove `telegram.bot_token` value,
   - set `accounts[0].imap_port` to non-number,
   - set `web_ui.bind` to `0.0.0.0` with `allow_lan=false`.
3. Run `run.bat` (or `run_mailbot.bat`) after each change.
4. Restore valid value and re-run.

**Pass evidence (logs/messages):**
- Error messages name exact key (example: `Ошибка в config.yaml: telegram.bot_token отсутствует`).
- Process exits non-zero before polling starts.

**Fail evidence:**
- Generic crash trace without field-level message.
- App starts with broken config.

**User-visible result:**
- User gets direct instruction about which key to fix.

---

## G3 — LAN UI safe enough for home LAN

**Definition:**
- Web UI must not start on non-loopback bind unless `allow_lan=true` and CIDR allowlist is non-empty.
- Access is blocked for non-allowlisted IPs.
- Password login is required for non-open routes.

**Manual test on clean Windows machine:**
1. Set `web_ui.bind: "0.0.0.0"`, `allow_lan: false`.
2. Start Web UI and verify startup refusal.
3. Set `allow_lan: true` and empty `allow_cidrs`; verify startup refusal.
4. Set `allow_cidrs` to local subnet (example `192.168.1.0/24`) and restart.
5. From allowlisted host: open Web UI and verify login page appears.
6. From non-allowlisted host: verify HTTP 403.
7. Try wrong password once and verify explicit login error.

**Pass evidence (logs/messages):**
- Refusal messages:
  - `web_ui.allow_lan=false: bind outside loopback refused`
  - `web_ui.allow_cidrs must be set when allow_lan=true`
- Runtime log when LAN enabled: `WEB_UI_LAN_ENABLED bind=... allow_cidrs=...`.
- Browser response `Forbidden` for blocked IP.

**Fail evidence:**
- UI starts on `0.0.0.0` without LAN guardrails.
- Non-allowlisted host can reach protected pages.

**User-visible result:**
- LAN users can log in from intended subnet only.

---

## G4 — Support bundle sufficient

**Definition:**
- `/doctor/export` must generate `diagnostics.zip` that includes redacted config, health snapshot, manifest status, and runtime versions.

**Manual test on clean Windows machine:**
1. Start app and open Web UI.
2. Log in and open `/doctor`.
3. Trigger export.
4. Inspect resulting `diagnostics.zip` contents.
5. Trigger export again immediately to verify cooldown behavior.

**Pass evidence (logs/messages):**
- Downloaded file name is `diagnostics.zip`.
- ZIP contains:
  - `config/config.redacted.yaml`,
  - `health/health.json`,
  - `build/manifest_status.json`,
  - `versions/runtime.txt`.
- Secret values are replaced with `***REDACTED***`.
- Repeated export under cooldown returns `Too many exports. Retry in ...` with HTTP 429.

**Fail evidence:**
- Missing required diagnostics files.
- Secrets present in exported config.

**User-visible result:**
- User can send one diagnostic archive to support without leaking credentials.

---

## G5 — Build reproducible enough (minimal)

**Definition:**
- One-folder build script runs from clean venv and produces `dist/MailBot` containing executable, runner, config example, quickstart, and manifest.

**Manual test on clean Windows machine:**
1. Create fresh `.venv`.
2. Install `requirements.txt` and `requirements-build.txt`.
3. Run `build_windows_onefolder.bat`.
4. Verify `dist\MailBot` exists.
5. Verify files:
   - `MailBot.exe`,
   - `run.bat`,
   - `config.example.yaml`,
   - `README_QUICKSTART_WINDOWS.md`,
   - `manifest.sha256.json`.
6. Launch `dist\MailBot\run.bat` and verify it prompts for config when missing.

**Pass evidence (logs/messages):**
- Build banner and `Build complete: ...\dist\MailBot`.
- No PyInstaller error.

**Fail evidence:**
- Missing `dist\MailBot` or missing manifest/config example/run script.

**User-visible result:**
- Operator can produce and run a distributable folder without manual file surgery.
