Goal (incl. success criteria):
- Ensure every processed email results in a valid, meaningful, delivered Telegram message with fallback, transactional delivery semantics, validation, and observability. Success: tests cover fallback, validation, retries, and TG stage is never done on failure.

Constraints/Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- Do not change pipeline order, payload schema (beyond content), or IMAP/LLM logic.
- Commit changes and create PR after.

Key decisions:
- Enforce summary quality with minimum length/word checks plus placeholder detection before allowing LLM payloads.
- Validate Telegram markup after sanitization and fall back on invalid payloads.

State:
- Implementing payload contract, fallback generator, and TG delivery retries.

Done:
- Added payload contract checks, deterministic fallback, and transactional TG send handling.
- Added observability events and metrics for payload validation and delivery.
- Added/updated unit tests for fallback, validation, and TG delivery retries.
- Tests: pytest mailbot_v26/tests/test_telegram_sender.py; pytest mailbot_v26/tests/test_telegram_payload_validation.py; pytest mailbot_v26/tests/test_telegram_payload_pipeline.py; pytest mailbot_v26/tests/test_telegram_delivery_pipeline.py.

Now:
- Finalize updates, review diffs, and prepare commit/PR.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- mailbot_v26/pipeline/processor.py
- mailbot_v26/pipeline/stage_telegram.py
- mailbot_v26/worker/telegram_sender.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/start.py
- mailbot_v26/observability/metrics.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- pytest mailbot_v26/tests/test_telegram_sender.py
- pytest mailbot_v26/tests/test_telegram_payload_validation.py
- pytest mailbot_v26/tests/test_telegram_payload_pipeline.py
- pytest mailbot_v26/tests/test_telegram_delivery_pipeline.py
