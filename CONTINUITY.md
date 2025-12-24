Goal (incl. success criteria):
- Implement Daily Digest v1 on top of Attention Gate: send a single daily Telegram digest per user with deferred email counts, commitment status, and trust/relationship deltas. Success: digest sends at most once per calendar day when content exists, persists last_digest_sent_at, logs [DAILY-DIGEST] decisions, uses existing Telegram sender, and tests cover digest send rules while payload stability tests remain green.

Constraints / Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- Do not change Telegram payload structure for normal emails, pipeline order, queue, IMAP, or LLM logic.
- Daily Digest is deterministic and read-only over deferred emails, commitments, trust deltas, and relationship health deltas.
- Failures must be logged and must not interrupt mail processing.

Key decisions:
- Store daily digest send state in digest_state.last_digest_sent_at keyed by account_email.
- Compute digest counts via CRM analytics queries and send via existing Telegram sender.
- Gate digest sending behind ENABLE_DAILY_DIGEST feature flag.

State:
- Daily Digest v1 implemented with CRM queries, digest_state persistence, and Telegram sender call.

Done:
- Added digest_state table and knowledge DB helpers for last_digest_sent_at.
- Added analytics queries for deferred counts, commitments, trust delta, and health delta.
- Added daily digest sender with [DAILY-DIGEST] decision logs and daily dedupe.
- Added tests for daily digest send rules.

Now:
- Daily Digest v1 implemented and tested.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/daily_digest.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/tests/test_daily_digest.py
- digest_state table
