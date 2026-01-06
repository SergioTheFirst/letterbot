Goal (incl. success criteria):
- Implement deterministic Behavior/Attention Engine with delivery modes, docs/ADR, config policy, events_v1 integration, TG/digest wiring, and green tests.
Constraints / Assumptions:
- Pipeline order unchanged (PARSE → LLM → TG; digests as-is).
- Telegram payload schema for outbound notifications unchanged.
- Fast First preserved for critical deliveries.
- Events_v1 remains the source of truth.
- RU-first user-facing responses.
- No new paid services.
- No stable thread key (thread_id/conversation_id or reliable in_reply_to mapping) available for shadow detectors.
Key decisions:
- Delivery decisions are deterministic and logged in events_v1 (DELIVERY_POLICY_APPLIED).
- Quiet hours use local machine time; no Telegram DND detection.
State:
- Behavior/Attention Engine integrated with TG delivery and daily digest.
- Delivery policy config and feature flags added.
- Events_v1 extended for behavioral signals.
- Premium processor routing available behind feature flag.
Done:
- Added behavior engine module and delivery decision policy (IMMEDIATE/BATCH/DEFER/SILENT).
- Added delivery policy config + feature flags (circadian, attention debt, surprise budget shadow).
- Extended events_v1 contract and emissions (delivery policy, attention debt, surprise).
- Integrated deferral into TG delivery and daily digest with deferred items section.
- Added ADRs + behavioral docs; updated STRATEGY.
- Added unit tests for policy config, attention engine, digest deferred items, surprise event.
- Added weekend batching rule for non-critical high-value emails (reason_code=weekend_batch).
- Added defensive fallback for behavior decision logic failures to preserve legacy delivery flow.
- Added premium processor feature flag with queue routing and fallback.
- Added thread key primitives, header plumbing, persistence columns, and tests.
- Added deadlock detector (shadow-only), policy config, and dedupe-backed events_v1 emission tests.
- Added silence-as-signal detector (shadow-only), config policy, digest hook, and tests.
- 2026-01-02: trust bootstrapping (digest-only) behind enable_trust_bootstrap.
Now:
- None.
Next:
- UNCONFIRMED
Open questions (UNCONFIRMED if needed):
- None.
Working set (files / tables / tests):
- mailbot_v26/behavior/attention_engine.py
- mailbot_v26/behavior/deadlock_detector.py
- mailbot_v26/behavior/silence_detector.py
- mailbot_v26/behavior/trust_bootstrap.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/config/delivery_policy.py
- mailbot_v26/config/deadlock_policy.py
- mailbot_v26/config/silence_policy.py
- mailbot_v26/config/trust_bootstrap.py
- mailbot_v26/storage/analytics.py
- mailbot_v26/pipeline/daily_digest.py
- mailbot_v26/pipeline/digest_scheduler.py
- mailbot_v26/feedback.py
- mailbot_v26/events/contract.py
- mailbot_v26/tests/test_attention_engine.py
- mailbot_v26/tests/test_delivery_policy_config.py
- mailbot_v26/tests/test_daily_digest_deferred.py
- docs/BEHAVIOR_ENGINE.md
- docs/DELIVERY_POLICY.md
- docs/ADR/ADR-004.md
- mailbot_v26/start.py
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/tools/config_bootstrap.py
- mailbot_v26/tests/test_premium_processor_routing.py
- mailbot_v26/tests/integration/harness.py
- mailbot_v26/tests/integration/test_degradation_scenarios.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- mailbot_v26/behavior/threading.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/tests/test_threading.py
- mailbot_v26/tests/test_threading_migration.py
- mailbot_v26/tests/test_threading_premium_integration.py
- mailbot_v26/tests/test_deadlock_detector.py
- mailbot_v26/tests/test_deadlock_premium_hook.py
- mailbot_v26/tests/test_silence_detector.py
- mailbot_v26/tests/test_silence_digest_hook.py
- mailbot_v26/tests/test_trust_bootstrap_metrics.py
2026-01-02: daily digest insights (deadlock+silence) behind enable_digest_insights.
2026-01-02: daily behavioral metrics block behind enable_behavior_metrics_digest.
2026-01-02: digest insight labels localized; i18n missing-key fallback returns empty.
2026-01-02: digest action templates behind enable_digest_action_templates.
2026-01-03: weekly accuracy report behind enable_weekly_accuracy_report.
2026-01-02: flow protection (focus hours) behind enable_flow_protection.
2026-01-03: weekly accuracy report behind enable_weekly_accuracy_report.
2026-01-03: regret minimization (commitments evidence) behind enable_regret_minimization.
2026-01-03: action templates gated by events accuracy behind enable_trust_bootstrap.
2026-01-03: weekly calibration (surprise breakdown) behind enable_weekly_calibration_report.
2026-01-03: uncertainty queue (digest-only) behind enable_uncertainty_queue.
2026-01-03: commitment chain digest (facts-only) behind enable_commitment_chain_digest.
2026-01-04: premium clarity v1 flag, emoji whitelist, selective render events.
2026-01-04: premium clarity confidence dots (auto/always/never).
2026-01-04: premium clarity facts baseline and line budget enforcement.
2026-01-05: premium clarity v1.1 provenance tags and why line.
- 2026-01-05: premium clarity v1.1 evidence-first gating, attachment line update, render event metadata, tests.
- 2026-01-06: premium clarity v1.2 attachment provenance disambiguation, numeric-safe attachment summaries, TG render flags.
- 2026-01-06: digest scheduler deduped by chat_id, daily_digest_sent payload includes chat_scope/account_emails, test added.
2026-01-07: verified chat_id digest dedup test coverage and daily_digest_sent payload fields.
2026-01-07: weekly_digest_sent now includes chat_scope/account_emails (chat_id dedup scope) for events_v1.
2026-01-07: scoped learning events (delivery_policy_applied/priority_correction/surprise) with chat_scope+account_emails for multi-account single user.
2026-01-07: scoped learning events (delivery_policy_applied/priority_correction/surprise) with optional chat_scope+account_emails when chat_id is configured.
2026-01-07: scoped learning events (delivery_policy_applied/priority_correction/surprise) with optional chat_scope+account_emails for single-user multi-account.
2026-01-08: weekly digest calibration now includes gated “рост точности” (events-only surprise-rate delta vs previous window).
2026-01-08: added optional chat_scope/account_emails to delivery_policy/correction/surprise events for single-user multi-account learning scope.
2026-01-08: unified chat_scope/account_emails propagation for surprise/correction/delivery events and restored scope test coverage.
2026-01-03: premium clarity confidence dots scale (5|10) config + tests.
2026-01-09: weekly analytics now aggregate by chat scope account_emails (single-user multi-account), tests.
2026-01-03: weekly digest добавил shadow-предложения к калибровке (events_v1).
2026-01-10: daily digest trust bootstrap + behavior metrics now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-10: quality metrics (daily+weekly) now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-04: daily digest uncertainty queue now aggregates by chat-scope account_emails (single-user multi-account).
2026-01-10: daily digest deferred+commitments now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-03: daily digest digest-insights (deadlock/silence) now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-03: daily+weekly digest attention economics + daily commitment-chain + regret-minimization now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-04: weekly digest base metrics now aggregate by chat-scope account_emails (single-user multi-account).
2026-01-04: silence detector now aggregates by chat-scope account_emails (single-user multi-account).
2026-01-04: updated CONSTITUTION.md (Подробнее button exception, emoji doc types, trust dots threshold) and enforced emoji whitelist.
2026-01-04: emoji whitelist now treats U+23xx as emoji for strict allowlist enforcement.
2026-01-05: processing spans + health snapshots stored in SQLite for observability (latency/LLM quality/system health), no UX change.
2026-01-05: observability analytics + export for processing spans (scoped by account_emails), PII-guarded.
2026-01-05: observability retention + size caps + stricter PII scrubbing for spans/health snapshots.
2026-01-11: local read-only web observability console added.
2026-01-12: removed quiet/focus/weekend delivery deferrals; DeliveryContext simplified; telegram sender returns message_id with edit helper.
2026-01-05: delivery SLA budget with minimal Tier-1 then edit-in-place.
2026-01-05: web health dashboard/API read-only with scoped account filters.

2026-01-06: web health default window=30 + system_mode persistence fix + web health tests.
2026-01-12: web events timeline (read-only, scoped, PII-guarded).
2026-01-13: relationship graph API+UI (read-only, scoped, deterministic, PII-guarded).
