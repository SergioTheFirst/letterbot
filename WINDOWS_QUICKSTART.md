# Windows Quickstart

1. Install Python 3.10+ and clone the repository.
2. Open the project folder.
3. Run `letterbot.bat`.
4. For the extracted release ZIP, use `run.bat`. The repository helper `run_dist.bat` is only for local pre-release dist checks.
5. Use `run_tests.bat` to execute the test suite.
6. The launcher creates `.venv` in the project root and runs `python -m mailbot_v26`.
7. Install packages through `.venv\Scripts\python -m pip ...`.

Optional offline path:
- On a connected machine: `.venv\Scripts\python -m pip download -r requirements.txt -d wheelhouse`
- On the target machine: `.venv\Scripts\python -m pip install --no-index --find-links wheelhouse -r requirements.txt`

> LAN mode exposes the web UI to your local network. Use a strong password.

## LAN mode (safe)
```ini
[web_ui]
enabled = true
bind = 0.0.0.0
port = 8787
allow_lan = true
allow_cidrs = 192.168.0.0/16
password = CHANGE_ME
prod_server = true
require_strong_password_on_lan = true
```

Narrow `allow_cidrs` to your subnet when possible.

Find the PC IP address with `ipconfig`, then open `http://<PC IPv4>:8787/` from another device.
Do not use `http://0.0.0.0:8787/` in the browser; that is a listen address, not a client URL.

If the page does not open, add a Windows Firewall rule:

`netsh advfirewall firewall add rule name="Letterbot Web UI 8787" protocol=TCP dir=in localport=8787 action=allow`

## Windows SmartScreen (first launch)
If `Letterbot.exe` shows `Windows protected your PC`, click `More info` -> `Run anyway`.

## Module-based commands
- Doctor diagnostics: `python -m mailbot_v26.doctor`
- Normal startup: `python -m mailbot_v26.start`
- Avoid direct script launch (`python start.py`) on Windows; module mode resolves package paths deterministically.
