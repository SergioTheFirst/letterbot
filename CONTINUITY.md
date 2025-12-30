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
- Attachment safety gates implemented with hard limits, truncation logging, and TG skip rendering.
- Anomaly alerts v0 implemented for response delay, frequency drop, and commitment proximity (feature-flagged).

Done:
- Added weekly_digest_state table and KnowledgeDB accessors.
- Added weekly digest analytics queries and formatting logic.
- Wired weekly digest into processor with safe logging.
- Added daily/weekly scheduler with time gating and SQLite dedupe.
- Added scheduler tests for daily/weekly due logic and error isolation.
- Unified TG UI Standard v27 implemented.
- Added attachment safety gate limits and extraction truncation logs.
- Added renderer handling for skipped attachments and binary leak suppression hardening.
- Added attachment safety gate tests.
- Added anomaly engine with analytics queries and preview/digest Signals blocks (feature-flagged).
- Added anomaly alert tests for response delay, frequency drop, and payload stability.
- Added attention economics analytics, weekly digest block, events, and tests under feature flag.
- Added runtime IMAP health manager with per-account backoff, alert dedupe, and persisted state.
- Added Event Contract v1 emitter/table with idempotent fingerprinting and basic pipeline integration.
- Added global GigaChat serialization lock via LLMRouter and regression guard tests.
- Introduced SystemOrchestrator v0 for policy logging and mode snapshots.
- Telegram delivery contract hardened to coerce DeliveryResult in legacy pipeline paths.
- Attachment extraction entrypoints aligned (defaults, exports) with safe error returns.
- Preview/telegram fallback text normalized to remove disallowed emoji while preserving priorities.
- MessageProcessor output formatting aligned with tests and summary contracts.
- pytest -q green after telegram/attachment contract fixes.
- SystemOrchestrator v1 введён.
- Analytics/digest switched to events_v1 as source-of-truth with email payload joins minimized.
- Event Contract v1 types normalized and emissions extended (digest, commitments, trust/health, TG, attention defer, attachments).
- Added idempotent events backfill tool with startup hook and observability markers.
- Added tests for events-backed analytics/digests and backfill idempotency.
- pytest -q green after events source-of-truth changes.
- Enforced GigaChat global lock in provider with wait logging and single-ingress guard tests.
- Trust v2 decay/redemption implemented from events_v1 with versioned snapshots, v2-preferred analytics, and tests added.

Now:
- UNCONFIRMED.

Next:
- Integration degradation tests.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/weekly_digest.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/pipeline/digest_scheduler.py
- mailbot_v26/pipeline/daily_digest.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/insights/anomaly_engine.py
- mailbot_v26/tests/test_anomaly_engine.py
- mailbot_v26/tests/test_telegram_payload_pipeline.py
- mailbot_v26/tests/test_weekly_digest.py
- mailbot_v26/tests/test_digest_scheduler.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/constants.py
- mailbot_v26/pipeline/tg_renderer.py
- mailbot_v26/tests/test_attachment_safety_gates.py
- mailbot_v26/mail_health/runtime_health.py
- mailbot_v26/tests/test_runtime_health.py
- mailbot_v26/events/contract.py
- mailbot_v26/events/emitter.py
- mailbot_v26/tools/backfill_events.py
- mailbot_v26/system/orchestrator.py
- mailbot_v26/pipeline/digest_scheduler.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tests/test_system_orchestrator.py
- tests/test_event_contract.py
- mailbot_v26/tests/test_events_source_of_truth.py
- mailbot_v26/llm/global_lock.py
- mailbot_v26/llm/providers.py
- mailbot_v26/llm/router.py
- mailbot_v26/tests/test_llm_single_ingress.py
- mailbot_v26/tests/test_gigachat_global_lock.py
- mailbot_v26/insights/trust_score.py
- mailbot_v26/observability/trust_snapshot.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tools/backfill_events.py
- mailbot_v26/config/config.ini
- mailbot_v26/tests/test_trust_score.py
- mailbot_v26/tests/test_daily_digest.py
