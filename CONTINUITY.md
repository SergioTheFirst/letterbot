Goal (incl. success criteria):
- Implement Telegram inbound handler (getUpdates polling) with callback handling, Russian command set, durable offset, SQLite feedback persistence/events_v1, override toggles, deterministic no-network tests; pytest -q green.

Constraints / Assumptions:
- Pipeline order unchanged (PARSE → LLM → TG; digests as-is).
- Telegram payload schema for outbound notifications unchanged.
- GigaChat single-flight only; no parallel calls.
- Inbound failures must not stop mail processing.
- Events_v1 is the source of truth for inbound corrections.
- RU-first user-facing responses.
- No new paid services.

Key decisions:
- Store inbound offset and runtime overrides in SQLite tables (telegram_inbound_state, runtime_overrides).
- Apply digest override in scheduler before feature flags.

State:
- Telegram inbound handler implemented and wired into main loop.

Done:
- Added telegram inbound module with polling client, command handling, callback parsing, and feedback persistence.
- Added runtime overrides store for digest toggles and applied it in digest scheduler.
- Added doctor read-only check helper for inbound /doctor command.
- Added inbound state persistence table and runtime overrides table.
- Added inbound tests (callbacks, commands, dedupe, polling offset).
- Added digest override test coverage.
- pytest -q green.

Now:
- None.

Next:
- UNCONFIRMED

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/telegram/inbound.py
- mailbot_v26/telegram/__init__.py
- mailbot_v26/storage/runtime_overrides.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/pipeline/digest_scheduler.py
- mailbot_v26/start.py
- mailbot_v26/doctor.py
- mailbot_v26/tests/test_telegram_inbound.py
- mailbot_v26/tests/test_digest_scheduler.py
