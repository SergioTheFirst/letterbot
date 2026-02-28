Goal (incl. success criteria):
- Enforce one-message rule reliability: no duplicate Telegram notifications for the same email (same email_id / same (account_id, imap_uid)) under retries, restarts, or duplicate IMAP ingestion.
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
- 2026-02-28: Block 2A (2.1/2.2/2.3) implemented with minimal diffs — Telegram action keyboard now includes `✓ Верно` (`mb:ok:<email_id>`) persisted as `priority_confirmation` in existing `priority_feedback` contour with graceful callback fail-safe; weekly digest accuracy line now renders as one compact row only when `priority_corrections >= 3`; new `/week` (`week`) command added with compact 7-day summary sourced from `KnowledgeAnalytics.weekly_compact_summary`; focused tests updated for keyboard/inbound/weekly gate+format/command.
- 2026-02-28: Block 1 package completed — Telegram inline keyboard now includes snooze menu (`⏰ Позже` → 2ч/6ч/Завтра) with persistent SQLite `telegram_snooze`, inbound callbacks schedule reminders, runtime loop delivers `📌 Напоминание` via existing telegram dedupe keys (`kind=snooze`), `/commitments` + `/tasks` command added from existing commitments data (deduped, limit 7), silence/deadlock defaults switched to enabled in settings templates, silence gate hardened (>=5 msgs in 30d + 14d cooldown default), deadlock cooldown default set to 7d, and regression tests added/updated; full pytest green (829 passed).
- 2026-02-28: Block 0A completed in one package — MessageProcessor now applies subject-line dedupe using shared compare helper (casefold/trim/space-collapse/RE-FW strip), action-line priority remains mail_type-first with `.xls` regressions (ACT/INVOICE override attachment fallback), clean_email external disclaimer cutoff (RU/EN markers) is enforced, and TG stage now always attaches inline action keyboard regardless of `enable_premium_processor`; targeted pytest suite green.
- 2026-02-28: Block 0 tasks 0.5+0.7 complete — Excel extractor dependency chain now includes `xlrd>=2.0.1` for `.xls`, extraction routing/fail-safe behavior covered by regression tests (`.xls`→xlrd, missing-xlrd no-crash, `.xlsx`→openpyxl), and `mailbot_v26/config/settings.ini.example` synchronized with `SETTINGS_TEMPLATE` plus parser/regression tests guarding template parity and int/bool parsing.
- 2026-02-28: Block 0 task 0.8 smoke coverage tightened — added fresh-clone regressions for missing `settings.ini` defaults in 2-file mode, migrate-config runtime section creation, and `settings.ini.example` inline-comment safety checks; targeted config/bootstrap and entrypoint pytest suites green.
- 2026-02-28: Block 0 tasks 0.3+0.4 complete — MessageProcessor action line priority fixed to mail_type-first (act/invoice/contract), subject/attachment/generic fallbacks tightened (no plural "таблицы" fallback), and clean_email disclaimer cutoff now removes RU/EN external-mail warning tails; regression tests added for action-line priority and disclaimer trimming.
- 2026-02-28: Task 0.9 complete — TG-stage One-Message Rule now enforced by persistent SQLite `telegram_delivery_log` atomic reservation/finalize/release guard keyed by `email:{email_id}` (kind-aware for snooze compatibility), duplicate skips emit `telegram_delivery_skipped_duplicate`, ingest dedupe normalizes account login, IMAP cursor keys normalized to prevent UID reappearance loops, and regression tests added for TG idempotency/rerun, case-insensitive UID dedupe, snooze key separation, and normalized IMAP state cursor.
- 2026-02-27: 2-file mode premium default updated — `enable_premium_processor` now falls back to `true` when absent in `settings.ini` (legacy mode behavior unchanged), templates updated, and feature-flag regression test added.
- 2026-02-27: legacy TG stage now attaches inline action keyboard (`reply_markup`) when premium flag is enabled in configured pipeline; delivery-pipeline regression test added.
- 2026-02-27: Telegram duplicate subject line suppression hardened via explicit `_normalize_subject_for_compare` helper (trim, whitespace collapse, casefold, RE/FW/FWD prefix stripping); renderer regression tests extended for duplicate vs non-duplicate lines.
- 2026-02-26: Telegram renderer now removes duplicate first body line when it matches header subject after normalization (trim/space-collapse/casefold/FW-RE chain stripping, including Cyrillic-safe compare); added renderer+processor regression tests for duplicate/non-duplicate/FW-RE/empty-subject cases.
- 2026-02-26: fixed Windows launcher pipe-parsing crash by removing `delims=|`/f-string pipe output from run_mailbot.bat, switched install_and_run.bat and update_and_run.bat to direct `mailbot_v26.doctor` + `mailbot_v26.start` invocations with explicit config-dir paths, updated settings.ini.example [web] inline docs, and added launcher/config regression tests.
- 2026-02-26: follow-up hardening after review: fixed batch retry counter expansion in run_mailbot.bat (enabled delayed expansion + !CONFIG_READY_ATTEMPTS!), tightened update_and_run.bat clean-tree check to include untracked files via git status --porcelain, and made doctor web busy-port guidance point to the active config-dir settings.ini path; regression tests updated and full pytest green.
- 2026-02-26: Windows first-run launcher loop hardened: run_mailbot.bat now re-opens exact CONFIG_DIR\accounts.ini and auto-retries config-ready up to 20 attempts with cancel guidance; update_and_run.bat now updates only on clean working tree via fetch + reset --hard origin/main and passes absolute config-dir to run stack; added tests for start config-dir isolation, run_stack web settings port propagation, and web main busy-port graceful exit.
- 2026-02-25: added two-layer anti-duplicate delivery protection: ingestion duplicate detection skips PARSE enqueue for existing (account_email, uid) with `duplicate_ingest_skipped` log, and TG stage is now idempotent via `emails.telegram_delivered_at` guard (`telegram_duplicate_skipped` + queue done). Added regression tests for duplicate UID ingest and forced duplicate TG job skip.
- 2026-02-25: added configurable web host/port via settings.ini [web] (2-file mode), wired web/runtime launcher defaults to settings.ini with legacy config.ini fallback, added non-fatal busy-port DEGRADED_NO_WEB stack behavior + actionable guidance, and doctor web host/port + port-availability WARN reporting with tests.
- 2026-02-25: fixed 2-file LLM regression for startup-health compatibility: router keeps accounts.ini precedence, but silently uses legacy keys.ini secrets when accounts.ini lacks provider secrets; startup health + llm loader regression tests are green.
- 2026-02-25: completed 2-file LLM source-of-truth tightening: in two-file mode llm primary/fallback mapping is read from accounts.ini, gigachat/cloudflare secrets are sourced from accounts.ini, and legacy keys.ini flow remains for legacy mode; regression tests added.
- 2026-02-25: hardened 2-file startup UX: update_and_run.bat is fail-open for git/pip and treats config-not-ready as guided setup (exit 0), LLM router now reads Cloudflare credentials from accounts.ini in two-file mode without keys.ini warning noise; launcher/LLM regression tests updated.
- 2026-02-24: fixed Windows 2-file launcher readiness flow: run_mailbot.bat now uses python `config-ready` (no global CHANGE_ME grep), re-checks after Notepad, exits 2 when config not ready; validate_config now validates IMAP/system sections separately; update_and_run.bat handles exit code 2 honestly; regression tests added.
- 2026-02-24: added onboarding gate to run_mailbot.bat — detects CHANGE_ME in
  accounts.ini after init-config, opens Notepad with instructions, exits cleanly;
  contract test added to lock init_config ↔ bat interface.
- 2026-02-24: corrected TROUBLESHOOTING_WINDOWS.md items 2-3 to two-file mode examples/validation (`settings.ini` + `accounts.ini`) and removed YAML-primary wording.
- 2026-02-24: consolidated setup docs to two-file mode (settings.ini + accounts.ini)
  as the only primary path; legacy config.ini/keys.ini/config.yaml guidance moved to
  TROUBLESHOOTING_WINDOWS.md legacy section; README/RUNNING/README_QUICKSTART updated.
- 2026-02-24: audited optional deps (langdetect/nltk/pyttsx3) for module-level import safety; no bare module-level imports found outside tests, so no guards/tests were required.
- 2026-02-24: confirmed dist_self_check behavior post-audit; dev-mode runtime check returns OK and does not surface optional-import errors.
- 2026-02-24: restored full-suite stability after import/web_ui fix: validate-config CLI now exits non-zero on warnings by default, compat report runs without requiring on-disk config.yaml and returns non-zero on schema failure, doctor uses legacy keys.ini token fallback in two-file mode diagnostics only, feature flags fall back to legacy config.ini when settings.ini is absent.
- 2026-02-24: fixed bare package imports in health_monitor.py and intelligence/__init__.py (ModuleNotFoundError on import; added regression tests in test_import_smoke.py).
- 2026-02-24: fixed config_yaml.py web_ui validator requiring bind/port when enabled=false; first-time minimal config now passes validation (regression tests added to test_validate_config_yaml.py).
- 2026-02-24: added python -m compileall mailbot_v26 step to ci_local.bat and ci.yml to catch bare-import bugs at CI time before they reach users.
- 2026-02-23: strict 2-file mode hardening: config-root resolution is config-dir-first, 2-file mode auto-activates when accounts.ini exists, startup/doctor skip legacy YAML/keys warnings in 2-file mode, LLM loader avoids YAML in 2-file mode and degrades safely on missing config, digest flags support settings.ini aliases (`daily_digest_enabled`/`weekly_digest_enabled`), and Windows launcher runs `python -m mailbot_v26.doctor --config-dir mailbot_v26\config`.
- 2026-02-23: unified config-dir-first startup/doctor flow: default `mailbot_v26/config`, no implicit repo-root `config.yaml` reads, 2-file primary mode (`settings.ini` + `accounts.ini`) with optional legacy fallbacks (`config.ini`/`keys.ini`), optional `config.yaml` info-only handling, LLM loader `dict`/`ConfigParser` int-read hardening, launcher `--config-dir` wiring, and regression coverage for no-root-reads + degraded Telegram mode.
- 2026-02-23: startup/doctor/config hardening v2: warning-first doctor by default with --strict gating, non-blocking launcher checks, 2-file config mode (`settings.ini` + `accounts.ini`) with legacy fallback reads, and migration helper with legacy backups.
- 2026-02-23: hardened YAML startup/doctor UX for Windows backslash parse errors with actionable guidance, added shared login normalization (strip+casefold+slash normalization for domain\user) across account lookups, and added regression tests for parse hint/single-quote success/case-insensitive domain login matching.
- 2026-02-23: unified config path resolution via `config/paths.py` (root `config.yaml` -> `mailbot_v26/config/config.yaml`), hardened INI readers for legacy no-section parsing + parse-error handling, removed startup YAML/example-file hard dependency, made pipeline processor import config-IO lazy, aligned batch entrypoint to `run_mailbot.bat`, and added regression tests for no import-time INI reads + malformed INI warning behavior.
- 2026-02-23: standardized Windows one-click chain (start_mailbot.bat -> run_mailbot.bat -> python -m mailbot_v26.start), added explicit required INI file checks with init-config hint, added start.py CLI (--help/--config-dir/--max-cycles), and added regression tests for no import-time config reads plus doctor/start behavior on missing/invalid INI.
- 2026-02-23: removed import-time config.ini reads from pipeline processor via lazy cached getters; auto-priority gate INI loader now supports guarded parse + legacy no-section mode + inline comments and deterministic defaults with warnings; added regression tests for malformed/legacy configs and processor import no-config-read; docs updated with Windows copy commands and added config.yaml.example.
- 2026-02-21: reformatted `config.ini.example` with Quick start + advanced headers, moved INI support toggle to `[support].enabled` (with legacy fallback), unified YAML/runtime support gating precedence (`support.enabled` > `features.donate_enabled`) for web + digest, updated docs, and added precedence regression tests.
- 2026-02-21: added shared guarded INI reader (`config/ini_utils.py`), refactored user INI loaders to deterministic fallback + one-time actionable warning, fixed `auto_priority_gate` import crash path, replaced `config.ini.example` with valid sectioned template, and added malformed/missing config.ini processor-import regression tests.
- 2026-02-21: added compact INI UX flow (new config.ini.compact.example + deterministic generator), INI support alias (`features.support`) with precedence over `features.donate_enabled`, and doctor hint with Windows copy command to compact template; tests added.
- 2026-02-21: doctor now uses build_bot_config with deterministic fallback/default BotConfig on YAML errors, loads priority/vip INI via fallback-safe loaders, and prints template+copy hints instead of crashing when config files are missing; added missing-config doctor-mode regression test.
- 2026-02-20: hardened priority config INI loading on Windows startup path; malformed/missing config.ini now logs actionable warning (template + Windows copy command) and deterministically falls back to defaults, with regression tests for malformed/missing files.
- 2026-02-20: completed donate toggle verification end-to-end (UI nav + /support route + digest P.S.), restored IMAP compatibility seams for deterministic polling tests, fixed learning timeline template/runtime errors, and stabilized run_stack --dry-run without local config dependency.
- 2026-02-20: implemented donate toggle `features.donate_enabled` in config examples/validator and gated support UI + TG support PS rendering with tests.
- 2026-02-20: fixed config validation robustness/order (accounts-first, supports accounts[].imap.* with top-level imap fallback, no-throw tuple contract) and removed forbidden audit wording from PDF extractor docs/comments.
- 2026-02-20: replaced user-facing product label "MailBot" -> "Letterbot" in web UI templates, launcher/help text, doctor/start banners, and related assertions; kept internal module/package identifiers unchanged.
- 2026-02-16: added config schema_version contract (default=1), validate-config --compat report, startup exit-code=2 on newer schema, docs/UPGRADE.md, and deterministic tests.
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
- 2026-02-16: unified app version source, added CLI version command, web footer version stamp, PyInstaller Windows version resource, SmartScreen docs, Keep-a-Changelog, dist contract checks, and deterministic version plumbing tests.
Now:
- Block 2A (2.1/2.2/2.3) delivered and regression-covered; monitor real-world usage of `✓ Верно`, `/week`, and weekly accuracy gate behavior.
Next:
- Monitor `priority_confirmation_recorded` volume vs `priority_correction_recorded` to validate visible-learning adoption.
- Collect early UX feedback on `/week` compact format and adjust labels only if clarity issues are reported.
- Watch weekly digest runs to confirm accuracy line appears only at corrections threshold (`>=3`) across account scopes.
Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: Is there an approved process to force-default-change for web_ui.password/api_token at install time for non-technical users?
Working set (files / tables / tests):
- mailbot_v26/pipeline/processor.py
- mailbot_v26/text/clean_email.py
- mailbot_v26/tests/test_pipeline_processor.py
- mailbot_v26/tests/test_clean_email.py
- mailbot_v26/bot_core/storage.py
- mailbot_v26/start.py
- mailbot_v26/imap_client.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- mailbot_v26/tests/test_imap_client.py
- mailbot_v26/features/flags.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/pipeline/tg_renderer.py
- mailbot_v26/start.py
- mailbot_v26/config/settings.ini.example
- mailbot_v26/tools/config_bootstrap.py
- mailbot_v26/tests/test_feature_flags.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- mailbot_v26/tests/test_tg_renderer.py
- mailbot_v26/pipeline/tg_renderer.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tests/test_tg_renderer.py
- mailbot_v26/tests/test_telegram_rendering_format.py
- run_mailbot.bat
- update_and_run.bat
- mailbot_v26/web_observability/app.py
- mailbot_v26/doctor.py
- mailbot_v26/tests/test_start_config_failures.py
- mailbot_v26/tests/test_run_stack.py
- mailbot_v26/tests/test_web_ui_main.py
- mailbot_v26/bot_core/storage.py
- mailbot_v26/start.py
- mailbot_v26/tests/test_telegram_delivery_pipeline.py
- mailbot_v26/tests/test_config_bootstrap.py
- tests/test_launcher_warning_first.py
- mailbot_v26/account_identity.py
- mailbot_v26/tests/test_account_identity.py
- start_mailbot.bat
- update_and_run.bat
- mailbot_v26/tests/test_start_config_failures.py
- mailbot_v26/tests/test_ini_runtime_guard.py
- mailbot_v26/config/auto_priority_gate.py
- mailbot_v26/config/ini_utils.py
- mailbot_v26/features/flags.py
- mailbot_v26/tools/make_ini_compact.py
- mailbot_v26/config/config.ini.compact.example
- mailbot_v26/tests/test_ini_compact_template.py
- mailbot_v26/tests/test_feature_flags.py
- mailbot_v26/doctor.py
- mailbot_v26/tests/test_doctor_mode.py
- tests/test_doctor_print_lan_url.py
- mailbot_v26/priority/priority_engine_v2.py
- mailbot_v26/tests/test_priority_engine_v2.py
- mailbot_v26/config/config.ini.example
- mailbot_v26/web_observability/templates/base.html
- mailbot_v26/web_observability/templates/support.html
- mailbot_v26/tests/test_daily_digest_support_telegram.py
- mailbot_v26/tests/test_web_support_page.py
- mailbot_v26/bot_core/extractors/pdf.py
- mailbot_v26/version.py
- mailbot_v26/__main__.py
- build/windows_version_info.txt
- docs/SMARTSCREEN.md
- CHANGELOG.md
- tests/test_cli_version.py
- tests/test_version_surfaces.py
- docs/UPGRADE.md
- tests/test_schema_compatibility.py
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

- mailbot_v26/telegram/decision_trace_ui.py
- mailbot_v26/telegram/inbound.py
- mailbot_v26/pipeline/weekly_digest.py
- mailbot_v26/ui/i18n.py
- mailbot_v26/tests/test_priority_keyboard.py
- mailbot_v26/tests/test_telegram_inbound.py
- mailbot_v26/tests/test_weekly_digest_accuracy_render.py
- mailbot_v26/tests/test_weekly_accuracy_report_queries.py
- SQLite table: priority_feedback