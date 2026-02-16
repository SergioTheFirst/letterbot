# Windows SmartScreen: what it means and what to do

## Why this warning appears
Windows SmartScreen is reputation-based protection. A newly built or rarely downloaded executable can trigger "Windows protected your PC" even when it is not malware.

This is expected for unsigned or newly published builds until reputation accumulates.

## Safe bypass flow (if you trust the source)
1. Start `MailBot.exe` or `run.bat` from the official release ZIP.
2. If SmartScreen blocks startup, click **More info**.
3. Confirm publisher/path, then click **Run anyway**.

## How to verify you downloaded the official ZIP
1. Unpack ZIP to a clean folder.
2. Run `verify_dist.bat` in the repository root (or `python -m mailbot_v26.tools.verify_dist dist/MailBot`).
3. Ensure output contains `VERIFY_DIST PASS` and `manifest status OK`.
4. Check file properties on `MailBot.exe` (Properties -> Details) and confirm ProductVersion/FileVersion match release notes.

## Signing status note
Code signing is optional in this project. Without signing, SmartScreen warnings may continue until enough users run the same binary and reputation is built.

If signing is added later, use Windows signature checks and publisher identity as an extra trust signal.
