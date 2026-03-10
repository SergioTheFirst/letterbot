## Quickstart (Windows)

### 1) Source mode (run from repository)

1. Open the repository folder.
2. Run `letterbot.bat`.
3. On first launch, Letterbot bootstraps config files in the repository root:
   - `settings.ini`
   - `accounts.ini`
4. Fill required values in `accounts.ini` (`bot_token`, `chat_id`, IMAP account fields).
5. Run `letterbot.bat` again.
6. Web UI is available at `http://127.0.0.1:8787/` when enabled.

### 2) Dist mode (one-folder build)

Current dist contract:

- source-repo dist helper: `run_dist.bat`
- extracted ZIP launcher: `run.bat`
- executable: `Letterbot.exe`
- integrity manifest: `manifest.sha256.json`
- config directory: `mailbot_v26\config\`
- runtime config files:
  - `mailbot_v26\config\settings.ini`
  - `mailbot_v26\config\accounts.ini`
- first launch creates missing config files from `*.example` in the same folder.

Steps:

1. Extract `Letterbot.zip`.
2. Open the extracted `Letterbot` folder.
3. Run `run.bat`.
4. Fill `mailbot_v26\config\accounts.ini`.
5. Run `run.bat` again.

### 3) Windows SmartScreen

For a new unsigned build, Windows may show a SmartScreen warning.
Use `More info` -> `Run anyway` for a trusted internal build.

### 4) Diagnostics

From source mode repository root:

- `python -m mailbot_v26 doctor --config-dir .`
- `python -m mailbot_v26 doctor --print-lan-url --config-dir .`

From dist mode folder:

- `Letterbot.exe doctor --config-dir mailbot_v26\config`
- `Letterbot.exe doctor --print-lan-url --config-dir mailbot_v26\config`
