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
- 2026-02-16: added Windows Smoke Kit docs (10 scenarios), Windows troubleshooting pack (15 symptom/cause/fix items), and tools/smoke_check.bat artifact generator for dev/dist triage.
- 2026-02-16: added doctor --print-lan-url, startup LAN/local URL logging (no 0.0.0.0 browsing URL), and Windows LAN/firewall docs + tests.
- 2026-02-16: web UI CSRF tokens enforced for POST login/doctor export; templates updated.
- 2026-02-16: web_ui config extended with prod_server + require_strong_password_on_lan and LAN password validation.
- 2026-02-16: prod_server waitress dependency gate added for web UI with clean-failure path and tests/docs updates.
- Added One-click Doctor export in Web UI (/doctor + diagnostics.zip with redaction and safe payload).
- Added behavior engine module and delivery decision policy (IMMEDIATE/BATCH/DEFER/SILENT).
- Added delivery policy config + feature flags (circadian, attention debt, surprise budget shadow).
- Extended events_v1 contract and emissions (delivery policy, attention debt, surprise).
- Integrated deferral into TG delivery and daily digest with deferred items section.
- Added ADRs + behavioral docs; updated STRATEGY.
- Added unit tests for policy config, attention engine, digest deferred items, surprise event.
- Added weekend delivery rule for non-critical high-value emails (legacy deferral now removed).
- Added defensive fallback for behavior decision logic failures to preserve legacy delivery flow.
- Added premium processor feature flag with queue routing and fallback.
- Added thread key primitives, header plumbing, persistence columns, and tests.
- Added deadlock detector (shadow-only), policy config, and dedupe-backed events_v1 emission tests.
- Added silence-as-signal detector (shadow-only), config policy, digest hook, and tests.
- 2026-01-02: trust bootstrapping (digest-only) behind enable_trust_bootstrap.
- 2026-01-15: web cockpit home with owner/engineer modes, status strip, and read-only analytics tests.
- 2026-01-16: health cockpit status page with basic/engineer modes, partial refresh, read-only indexes, and login selector/tests.
- 2026-01-08: web archive + email forensics views added with deterministic ordering, new indexes, screenshot helper hardening, and tests.
- IMAP UID bootstrap cursor behavior verified with criteria-aware tests.
- 2026-01-17: web observability masks sender labels and preview content in UI.
- 2026-01-10: cockpit budgets + triage lanes API (read-only) and tests.
- 2026-01-19: budget percentile anchoring hardened; regression tests added.
- 2026-01-20: post-start-only ingest gate with UTC filtering, optional allow_prestart_emails, updated IMAP tests.
- Added config.example.yaml and config.yaml gitignore entry; YAML config loader with validation and hot-reload.
- Added multi-account prefixing and YAML-driven LLM provider config (Cloudflare/GigaChat).
- Added validate_config unit tests for config.yaml rules.
- 2026-01-23: run_mailbot.bat config bootstrap, simplified install_and_run.bat, config path priority, pytest.ini, missing-config subprocess test.
- 2026-02-05: ensured PyYAML dependency in requirements for yaml import.
- 2026-02-05: requirements source unified in repo root with PyYAML>=6.0; bat scripts use venv python for pip/pytest.
- Windows launchers consolidated (root run_mailbot.bat, thin wrapper, venv python, config bootstrap, health checks, simplified install_and_run, quickstart steps).
- 2026-02-09: sentence-level TG dedup per field, message-level line dedup, regression tests.
- Added PyInstaller one-folder build script/spec, dist run.bat, and tamper-evident manifest checks with tests.
- Added repo-scoped Codex skills and index under .codex/skills.
- 2026-02-XX: web UI LAN allowlist, config.yaml web_ui settings, CIDR gate, and docs/tests updates.
- Added GitHub Actions CI workflow for tests and Windows one-folder build artifacts.
- 2026-02-10: added ci_local.bat as offline-first local CI runner (pip runtime+build deps, compileall, pytest, one-folder build, dist artifact checks).
- 2026-02-16: added production-readiness docs (stress audit for 1000 installs, production gates, Windows release checklist) under docs/.
- 2026-02-16: dependency guard added (importlib.find_spec), yaml/imapclient lazy imports, clean-failure entrypoint handling, and missing-deps tests.
- 2026-02-16: migrated legacy web tests to shared CSRF helper (browser-realistic login + doctor export token flow).
- 2026-02-16: added read-before-write repo skill and indexed it in .codex/skills/README.md.
- 2026-02-16: added support config block, authenticated /support web page, and optional rate-limited TG digest PS.
- 2026-02-16: release-core QA: IMAP tests patch unified on _imap_client_cls seam; polling/main entrypoint tests now stub dependency guard for deterministic no-yaml test runs.
- 2026-02-16: attachment extraction XLSX policy set to OPTIONAL in tests (openpyxl-specific assertion guarded with pytest.importorskip).
- 2026-02-16: formalized one-folder release artifact contract, added deterministic verify_dist post-build check, dist runtime missing-files self-check, and Windows docs SmartScreen/LAN/firewall updates with tests.
Now:
- Smoke kit + triage pack delivered with artifact folder output for first-line support.
Next:
- Validate smoke_check.bat behavior on clean Windows host in dev mode and dist-only mode.
Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: Is there an approved process to force-default-change for web_ui.password/api_token at install time for non-technical users?
Working set (files / tables / tests):
- docs/SMOKE_TESTS_WINDOWS.md
- docs/TROUBLESHOOTING_WINDOWS.md
- tools/smoke_check.bat
- docs/RELEASE_ARTIFACT_CONTRACT.md
- verify_dist.bat
- mailbot_v26/tools/verify_dist.py
- mailbot_v26/dist_self_check.py
- mailbot_v26/tests/test_dist_self_check.py
- mailbot_v26/doctor.py
- mailbot_v26/tools/networking.py
- tests/test_doctor_print_lan_url.py
- RUNNING.md
- docs/WINDOWS_QUICKSTART.md
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
- mailbot_v26/web_observability/app.py
- mailbot_v26/web_observability/templates/archive.html
- mailbot_v26/web_observability/templates/bridge.html
- mailbot_v26/web_observability/templates/cockpit.html
- mailbot_v26/web_observability/templates/email_detail.html
- mailbot_v26/web_observability/templates/latency.html
- mailbot_v26/tools/capture_web_screenshot.py
- mailbot_v26/tests/test_web_archive_forensics.py
- mailbot_v26/tests/test_web_cockpit_home.py
- mailbot_v26/tests/test_attachment_extraction.py
- mailbot_v26/tests/test_imap_client.py
- mailbot_v26/tests/test_main_entrypoint.py
- mailbot_v26/tests/test_polling_loop.py
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
- mailbot_v26/imap_client.py
- mailbot_v26/config_loader.py
- mailbot_v26/features/flags.py
- config.example.yaml
- .gitignore
- build_windows_onefolder.bat
- pyinstaller.spec
- run_dist.bat
- requirements-build.txt
- mailbot_v26/integrity.py
- mailbot_v26/tests/test_integrity_manifest.py
- README_QUICKSTART_WINDOWS.md
- README.md
- requirements.txt
- mailbot_v26/config_yaml.py
- mailbot_v26/llm/router.py
- mailbot_v26/llm/providers.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/health/mail_accounts.py
- mailbot_v26/mail_health/runtime_health.py
- tests/test_validate_config_yaml.py
- mailbot_v26/config/config.ini
- mailbot_v26/tools/config_bootstrap.py
- mailbot_v26/tests/test_premium_processor_routing.py
- mailbot_v26/tests/test_imap_client.py
- mailbot_v26/tests/integration/harness.py
- mailbot_v26/tests/integration/test_degradation_scenarios.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- mailbot_v26/behavior/threading.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/tests/test_threading.py
- mailbot_v26/web_observability/flask_stub.py
- mailbot_v26/tools/run_stack.py
- mailbot_v26/tests/test_web_ui_cidr.py
- RUNNING.md
- docs/WINDOWS_QUICKSTART.md
- mailbot_v26/tests/test_threading_migration.py
- mailbot_v26/tests/test_threading_premium_integration.py
- mailbot_v26/tests/test_deadlock_detector.py
- mailbot_v26/tests/test_deadlock_premium_hook.py
- mailbot_v26/tests/test_silence_detector.py
- mailbot_v26/tests/test_silence_digest_hook.py
- mailbot_v26/tests/test_trust_bootstrap_metrics.py
- mailbot_v26/tests/test_web_cockpit_budgets.py
- mailbot_v26/budgets/importance.py
- mailbot_v26/tests/test_budget_gate.py
- mailbot_v26/tests/test_budget_percentile_anchor.py
- run_mailbot.bat
- mailbot_v26/run_mailbot.bat
- install_and_run.bat
- README_QUICKSTART_WINDOWS.md
- mailbot_v26/start.py
- config.example.yaml
- pytest.ini
- tests/test_deprecated_bat_wrapper_docs_only.py
- tests/test_start_config_missing.py
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
2026-01-12: removed focus/weekend delivery deferrals; DeliveryContext simplified; telegram sender returns message_id with edit helper.
2026-01-05: delivery SLA budget with minimal Tier-1 then edit-in-place.
2026-01-05: web health dashboard/API read-only with scoped account filters.

2026-01-06: web health default window=30 + system_mode persistence fix + web health tests.
2026-01-12: web events timeline (read-only, scoped, PII-guarded).
2026-01-13: relationship graph API+UI (read-only, scoped, deterministic, PII-guarded).
2026-01-13: web attention economics API+UI (read-only, scoped, deterministic, PII-guarded).
2026-01-12: learning observatory (events_v1-derived, read-only web+API) added.
2026-01-13: delivery policy limited to IMMEDIATE; batching/deferral removed.
2026-01-14: immediate delivery enforced; time-window/batch configs removed.

- 2026-01-14: web observability console now localhost-only read-only with silent empty states.
2026-01-06: premium base template + CSS tokens; login/latency pages now use base; noisy empty-state copy removed.

- events narrative page + screenshot harness hardened
- Excel extractor: removed pandas dependency, added hard limits.
One-click run_mailbot.bat (start.py); start_mailbot.bat wraps run_mailbot.bat.
start.py loads config.yaml relative to start.py (module dir or repo root); standard logger kwargs removed.

Commitments ledger + evidence citations (PII-safe, cached, indexed)
- 2026-01-18: web cockpit lanes for bridge/archive/events (read-only, cached).
Attention economics v1 (explainable, cached, CSV export, indexed).
- 2026-01-10: Resource Budget Gate v1 (token tracking, yearly GigaChat freemium, top-20% LLM gating, FIFO queue, offline-first).
- 2026-01-10: DecisionTraceV1 emission foundation (events_v1).
- 2026-01-10: explainability surfaces (Telegram inline details, web decision trace API/UI, code scrubbing, failure log, explain-code histogram cache).
- 2026-01-20: Telegram inline priority correction menu with edit-in-place.
- 2026-01-20: Priority calibration report (cached) + drift warnings and web endpoint.
- 2026-01-20: Decision-trace health endpoint with emitter snapshot and drop-log tail.
- 2026-01-21: maintenance-mode indexes, decision-trace failure log hardening, post-start ingest rule clarified.
- 2026-01-22: telegram renderer semantic dedupe gates and golden tests.
