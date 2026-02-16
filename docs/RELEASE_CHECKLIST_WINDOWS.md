# Release Checklist — Windows one-folder zip

## 1) Clean build
1. Open terminal in repo root.
2. Ensure `.venv` exists and is active (or use `.venv\Scripts\python.exe`).
3. Install runtime deps:
   - `python -m pip install -r requirements.txt`
4. Install build deps:
   - `python -m pip install -r requirements-build.txt`
5. Run syntax check:
   - `python -m compileall mailbot_v26`
6. Run tests:
   - `pytest -q`
7. Run build:
   - `build_windows_onefolder.bat`

## 2) Config example placement and config creation
1. Confirm repository root contains `config.example.yaml`.
2. Build script must copy it to `dist\MailBot\config.example.yaml`.
3. End-user workflow:
   - first launch `dist\MailBot\run.bat`,
   - script auto-copies `config.example.yaml` to `config.yaml` if missing,
   - user edits `config.yaml` in Notepad and re-runs launcher.

## 3) Build one-folder zip artifact
1. After successful build, open `dist\MailBot`.
2. Verify required files exist:
   - `MailBot.exe`
   - `run.bat`
   - `config.example.yaml`
   - `README_QUICKSTART_WINDOWS.md`
   - `manifest.sha256.json`
3. Create release zip from folder contents:
   - Example PowerShell: `Compress-Archive -Path .\dist\MailBot\* -DestinationPath .\dist\MailBot-win-onefolder.zip -Force`

## 4) Smoke test (release candidate)
1. Unzip artifact to clean folder (not repo root).
2. Run `run.bat`.
3. Fill `config.yaml` with real credentials.
4. Re-run `run.bat`.
5. Validate:
   - mail fetch works (send test email to configured mailbox),
   - Telegram notification arrives,
   - Web UI opens on configured bind/port and login works,
   - `/doctor/export` downloads `diagnostics.zip`.

## 5) Known limitations
- Linux CI output does not represent Windows launcher behavior directly.
- `pytest -q` can fail in environments without required runtime dependencies (`yaml`, `imapclient`).
- Launcher assumes writable working directory for `config.yaml` creation.
- Manifest is tamper-evidence only, not signature-based trust.
- Web UI has password login + CIDR gating, but deployment owner remains responsible for router/NAT/firewall exposure.
