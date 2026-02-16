# UPGRADE (Windows one-folder ZIP)

## Recommended method
1. Download the new MailBot ZIP.
2. Unzip it into a NEW folder.
3. Copy your old `config.yaml` into the NEW folder.
4. Run `run_mailbot.bat` (or `run_dist.bat`) and confirm config validation is OK.

## Common mistakes (10)
1. Do not unzip over the old folder.
2. Do not edit `MailBot.exe`.
3. Do not edit `manifest.sha256.json`.
4. Do not remove `schema_version` from `config.yaml`.
5. Do not set `schema_version` to text.
6. Do not use tabs in YAML indentation.
7. Do not keep duplicate keys in `config.yaml`.
8. Do not browse to `http://0.0.0.0:<port>/`.
9. Do not copy an incomplete config from chat screenshots.
10. Do not ignore `validate-config --compat` hints before production run.

## Recovery if config is broken
1. Rename broken file to `config.bad.yaml`.
2. Copy `config.example.yaml` to `config.yaml`.
3. Re-enter values manually and keep the same indentation style.
4. Run `python -m mailbot_v26 validate-config --compat`.
