---
name: windows-launcher-standard
description: "Windows launcher standardization for MailBot. Use when editing any .bat or run scripts: enforce root runner, thin wrapper, venv python usage, config bootstrap UX, and forbid %I in .bat."
---

1) Treat repo-root run_mailbot.bat as the source of truth; mailbot_v26/run_mailbot.bat must remain a thin wrapper.
2) Use `cd /d "%~dp0"`, `setlocal enableextensions`, `chcp 65001`, and `PYTHONUTF8=1` patterns.
3) Use `.venv\Scripts\python.exe` consistently for Python invocations.
4) If any for loops exist, enforce `%%` variable syntax.
5) Enforce config bootstrap: copy config.example.yaml to config.yaml, open notepad, then exit.
6) Perform a static audit of .bat content to ensure no stray words execute as commands.
