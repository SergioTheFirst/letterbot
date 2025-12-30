Goal (incl. success criteria):
- Release wrapper Premium v1: one-command Windows install/run/tests, doctor diagnostics, version/changelog, predictable upgrade; success criteria: doctor prints/sends report without polling, CLI --version works, .venv scripts standard, pytest -q green.

Constraints / Assumptions:
- No pipeline reorder; main email notifications remain untouched.
- Telegram payload schema must not change.
- GigaChat stays strictly single in-flight request (global lock).
- No new paid LLM services or heavy local models.
- Doctor mode is read-only for mail processing state.

Key decisions:
- Add mailbot_v26/version.py with __version__ and show version at start/launch report.
- Add CLI dispatch in mailbot_v26.__main__ for doctor and --version.
- Doctor uses read-only SQLite checks and existing healthcheck hooks.
- Windows scripts standardized around root .venv and python -m mailbot_v26.

State:
- Release wrapper implemented: doctor diagnostics, version/changelog, Windows scripts, acceptance docs, and config bootstrap/validation.

Done:
- added Fast First Principle to Constitution
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
- Added integration degradation tests A–G and single-cycle harness (mailbot_v26/tests/integration).
- Added doctor diagnostics CLI and tests with read-only checks.
- Added version module, --version CLI flag, and launch report version display.
- Standardized Windows bootstrap scripts for .venv install/run/tests.
- Added CHANGELOG.md, WINDOWS_QUICKSTART.md, and acceptance checklist.
- Added update_and_run.bat with doctor gate before polling loop.
- Added backup/restore/export CLI and Windows wrappers.
- Added backup/restore/export tooling with retention and redaction.
- Added tests for backup/restore smoke, doctor gate, and export determinism.
- Added init-config and validate-config CLI for template creation and config checks.
- Updated Windows scripts to gate on config init, doctor, and validation before polling.
- Added acceptance checklist and run_acceptance.bat.
- Deprecated legacy Windows bat scripts with wrappers.
- Added tests for config bootstrap, validation rules, and deprecated bat wrapper.
- Premium Telegram View v1 rendering: attachment type summary line, bold-italic body summary, and premium attachment snippet formatting.
- Mail Type Hierarchy v2: deterministic subtype refinement with reason codes, feature flag, pipeline logs, and tests.
- PriorityEngineV2 rule-based scoring wired in shadow mode with vip sender config, structured logs, and unit tests; pytest -q green.

Now:
- UNCONFIRMED.

Next:
- UNCONFIRMED.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/doctor.py
- mailbot_v26/version.py
- mailbot_v26/__main__.py
- mailbot_v26/start.py
- mailbot_v26/health/mail_accounts.py
- tests/test_doctor_mode.py
- tests/test_cli_version.py
- mailbot_v26/tests/test_main_entrypoint.py
- install_and_run.bat
- run_mailbot.bat
- run_tests.bat
- run_acceptance.bat
- CHANGELOG.md
- WINDOWS_QUICKSTART.md
- ACCEPTANCE.md
- docs/ACCEPTANCE_CHECKLIST.md
- update_and_run.bat
- open_config_folder.bat
- backup.bat
- restore.bat
- update.bat
- mailbot_v26/run_mailbot.bat
- mailbot_v26/tools/backup.py
- mailbot_v26/tools/restore.py
- mailbot_v26/tools/export_data.py
- mailbot_v26/tools/config_bootstrap.py
- tests/test_backup_restore_smoke.py
- tests/test_update_guard_doctor_gate.py
- tests/test_export_determinism.py
- tests/test_init_config_creates_templates.py
- tests/test_validate_config_account_id_rules.py
- tests/test_deprecated_bat_wrapper_docs_only.py
- mailbot_v26/pipeline/tg_renderer.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tests/test_tg_renderer.py
- mailbot_v26/tests/test_telegram_rendering_format.py
- mailbot_v26/tests/test_telegram_payload_pipeline.py
- mailbot_v26/tests/test_telegram_payload_validation.py
- mailbot_v26/tests/test_telegram_render_modes.py
- tests/test_tg_payload_pipeline.py
- mailbot_v26/domain/mail_type_classifier.py
- mailbot_v26/features/flags.py
- mailbot_v26/config/config.ini
- mailbot_v26/tools/config_bootstrap.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tests/test_mail_type_hierarchy.py
- mailbot_v26/priority/priority_engine_v2.py
- mailbot_v26/tests/test_priority_engine_v2.py
