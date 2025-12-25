Goal (incl. success criteria):
- Deliver proactive daily/weekly digests on schedule; success criteria: scheduler tick independent of new mail, config time gating, SQLite dedupe, Telegram delivery, and tests updated.

Constraints / Assumptions:
- No pipeline reorder; main email notifications remain untouched.
- Digest must fail safely and never block mail processing.
- Use SQLite weekly_digest_state with ISO year-week key.
- Escape all dynamic Telegram HTML fields.

Key decisions:
- Weekly digest schedule read from [weekly_digest] config (weekday/hour/minute).
- Daily digest schedule read from [daily_digest] config (hour/minute).
- Attention economics computed from stored body_summary/subject word counts (200 wpm).
- Trust deltas derived from trust_snapshots within last 7 days.
- Emit weekly_digest_sent/skipped/failed via EventEmitter.

State:
- Digest scheduler module implemented and wired to the main loop.
- Processor no longer triggers digests; scheduling is handled separately.

Done:
- Added weekly_digest_state table and KnowledgeDB accessors.
- Added weekly digest analytics queries and formatting logic.
- Wired weekly digest into processor with safe logging.
- Added daily/weekly scheduler with time gating and SQLite dedupe.
- Added scheduler tests for daily/weekly due logic and error isolation.

Now:
- Proactive digest scheduler is active and gated by feature flags.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/weekly_digest.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/pipeline/digest_scheduler.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/tests/test_weekly_digest.py
- mailbot_v26/tests/test_digest_scheduler.py
