Goal (incl. success criteria):
- Deliver weekly digest v27.x with deterministic, LLM-free summary; success criteria: feature flag gating, ISO week dedupe, Telegram HTML output, event logs, and tests updated.

Constraints / Assumptions:
- No pipeline reorder; main email notifications remain untouched.
- Digest must fail safely and never block mail processing.
- Use SQLite weekly_digest_state with ISO year-week key.
- Escape all dynamic Telegram HTML fields.

Key decisions:
- Weekly digest schedule read from [weekly_digest] config (weekday/hour/minute).
- Attention economics computed from stored body_summary/subject word counts (200 wpm).
- Trust deltas derived from trust_snapshots within last 7 days.
- Emit weekly_digest_sent/skipped/failed via EventEmitter.

State:
- Weekly digest module, analytics helpers, schema, and processor integration implemented.
- Feature flag ENABLE_WEEKLY_DIGEST added (default False) with config defaults.
- Weekly digest tests added for dedupe, empty data, and flag off.

Done:
- Added weekly_digest_state table and KnowledgeDB accessors.
- Added weekly digest analytics queries and formatting logic.
- Wired weekly digest into processor with safe logging.

Now:
- Weekly digest feature is implemented and gated by config flag.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/weekly_digest.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/tests/test_weekly_digest.py
