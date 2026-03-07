## Quickstart (Windows)

### 1) Source mode (run from repository)

1. Open the repository folder.
2. Run `letterbot.bat`.
3. On first launch, Letterbot bootstraps config files in the **repository root**:
   - `settings.ini`
   - `accounts.ini`
4. Fill required values in `accounts.ini` (`bot_token`, `chat_id`, IMAP account fields).
5. Run `letterbot.bat` again.
6. Web UI is available at `http://127.0.0.1:8787/` (if enabled in config).

### 2) Dist mode (one-folder build)

Current dist contract (from build scripts):

- launcher: `run_dist.bat`
- executable: `Letterbot.exe`
- config directory: `mailbot_v26\config\`
- config files used at runtime:
  - `mailbot_v26\config\settings.ini`
  - `mailbot_v26\config\accounts.ini`
- first launch creates those files from examples in the same `mailbot_v26\config\` folder.

Steps:

1. Extract `Letterbot.zip`.
2. Open `dist\Letterbot` contents.
3. Run `run_dist.bat`.
4. Fill `mailbot_v26\config\accounts.ini`.
5. Run `run_dist.bat` again.

### 3) Windows SmartScreen

For a new unsigned build, Windows may show SmartScreen warning.
Use **More info → Run anyway** for trusted internal build artifacts.

### 4) Diagnostics

From source mode repository root:

- `python -m mailbot_v26 doctor --config-dir .`
- `python -m mailbot_v26 doctor --print-lan-url --config-dir .`

From dist mode folder:

- `Letterbot.exe doctor --config-dir mailbot_v26\config`
- `Letterbot.exe doctor --print-lan-url --config-dir mailbot_v26\config`
