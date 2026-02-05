# Running MailBot (one-click)

1. Double-click `run_mailbot.bat` in the repository root.

## Troubleshooting
- **Port in use**: stop the process using the port and retry.
- **DB locked**: close any other MailBot processes that might be using `data/mailbot.sqlite`.
- **Wrong password**: update `web_ui.password` in `config.yaml` and restart.
