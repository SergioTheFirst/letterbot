# Running MailBot (one-click)

1. Double-click `run_mailbot.bat` in the repository root.

## Troubleshooting
- **Port in use**: stop the process using the port and retry.
- **DB locked**: close any other MailBot processes that might be using `data/mailbot.sqlite`.
- **Wrong password**: update `[general] web_password` in `mailbot_v26/config/config.ini` and restart.
