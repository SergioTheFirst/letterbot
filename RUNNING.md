# Running MailBot (one-click)

1. Double-click `start_mailbot.bat` in the repository root.
2. Open http://127.0.0.1:8111 in your browser.
3. Use the password from `[general] web_password` in `mailbot_v26/config/config.ini`.
4. Bridge cockpit lanes are available via `?lane=critical` on the web console.

## Troubleshooting
- **Port in use**: change the port with `--port` in `start_mailbot.bat`, or stop the process using the port.
- **DB locked**: close any other MailBot processes that might be using `data/mailbot.sqlite`.
- **Wrong password**: update `[general] web_password` in `mailbot_v26/config/config.ini` and restart.
