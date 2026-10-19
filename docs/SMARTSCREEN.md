# Windows SmartScreen (LetterBot.ru installer)

1. Start `Letterbot-Setup.exe` from the official release package.
2. If Windows shows `Windows protected your PC`, click `More info` -> `Run anyway`.
3. Verify package integrity after install: the web UI Doctor page shows the
   `manifest.sha256.json` status (OK = installed files match the release).
   From a source checkout: `verify_dist.bat` or
   `python -m mailbot_v26.tools.verify_dist dist/Letterbot`.
   The manifest check detects damaged or partially updated files; it is not a
   security control, so download the installer only from the official release page.
4. After install, check `Letterbot.exe` file properties and confirm ProductVersion/FileVersion match the release notes.
