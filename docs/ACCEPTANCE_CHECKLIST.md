# Acceptance checklist

1. `letterbot.bat` creates `.venv`, installs dependencies, and starts `python -m mailbot_v26.tools.run_stack --config-dir . --no-browser`.
2. Extracted release `run.bat` bootstraps dist config and starts `Letterbot.exe` (`run_dist.bat` remains the repository helper before packaging).
3. `run_tests.bat` runs `python -m pytest -q` using `.venv`.
4. `python -m mailbot_v26 --version` prints the current version.
5. Launch report includes the version string.
6. Doctor report prints to console and sends one Telegram message.
7. Doctor mode does not start polling or write mail processing state.
8. Telegram delivery contract remains stable (payload schema unchanged).
9. LLM fallback works and the GigaChat global lock enforces single in-flight call.
10. IMAP account backoff logic still isolates failures.
11. Digest scheduler does not reorder pipeline stages.
12. `pytest -q` passes locally.
13. Integration degradation scenarios remain green.
14. `accounts.ini` validation flags missing `host`, `port`, `use_ssl`, and `chat_id`.
