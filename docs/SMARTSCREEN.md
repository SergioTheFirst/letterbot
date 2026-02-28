# Windows SmartScreen (Letterbot one-folder)

1. Start `Letterbot.exe` or `run.bat` from the official release ZIP.
2. If Windows shows “Windows protected your PC”, click `More info` → `Run anyway`.
3. Verify package integrity:
   - `verify_dist.bat`
   - or `python -m mailbot_v26.tools.verify_dist dist/Letterbot`
4. Check `Letterbot.exe` file properties and confirm ProductVersion/FileVersion match release notes.
