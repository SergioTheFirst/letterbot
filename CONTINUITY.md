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
