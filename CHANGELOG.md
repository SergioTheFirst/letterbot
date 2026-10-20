# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [28.0.0] - 2026-03-21

### Added
- English (EN) locale support: DEFAULT_LOCALE = "en", full EN i18n
  catalog (92 keys), humanize_* dispatch by locale, /langen /langru
  /lang en|ru bot commands.
- Bilingual golden corpus 120/120 (60 EN + 60 RU).
- configure_processor_locale() wired from startup and /lang command.

### Changed
- Premium Telegram UX: single message per email, edit-in-place
  priority updates, watermark in digests only.
- Weekly digest format: calm human-first layout with prioritised
  bullet points.
- Bounded sender-memory personalisation (45-day window, ±12 bias).
- Default locale in settings.ini.example set to en.

### Fixed
- Processor locale now propagates on /lang command at runtime.
- Priority callback always answers and edits in-place (no second
  message).
- Startup DB parent directory created before Storage().
- Delivery SLA edit-in-place: no resend on edit failure.

### Security
- None.

## [Unreleased]

### Added
- Single-file Windows installer `dist/Letterbot-Setup.exe` built on top of the verified `dist/Letterbot` one-folder bundle; ships only manifest-listed files, refuses to run while Letterbot is running, and shows its errors (log: `%TEMP%\Letterbot-install.log`).

### Changed
- CI publishes both `Letterbot.zip` and the smoke-installed `Letterbot-Setup.exe`; locally the installer is built only with `LETTERBOT_BUILD_INSTALLER=1`.

### Fixed
- Packaged `Letterbot.exe` now reads feature flags, policy settings, backup/restore/export/cleanup paths and the doctor manifest from `<exe>\mailbot_v26\config` / the exe folder instead of `_internal`.
- Startup with an unwritable data folder exits with a clear message instead of a traceback.
- Letterbot no longer marks incoming emails as read (mailbox is opened read-only).
- No more duplicate-notification storm when the queue dispatcher hits an error; a broken snooze row no longer stops queue processing.
- Daily and weekly digests are no longer sent on days/weeks without content, follow `/langru`, and escape sender/subject text.
- Telegram button edits that Telegram rejects are no longer reported as "Priority updated"; "message is not modified" counts as success.
- Pipeline caches are released on every terminal path (no memory growth from duplicate or failed emails).
- Backup takes a consistent database snapshot; restore refuses while Letterbot runs, never overwrites config with masked `***` values and removes stale `-wal`/`-shm` files.
- Config files saved by Notepad as "UTF-8 with BOM" are read correctly; ANSI files give a clear error instead of silently loading defaults.
- Launch scripts (`run.bat`, `update_and_run.bat`, `open_config_folder.bat`) are ASCII-only and the config retry loop no longer reopens Notepad after a correct edit.

- No more lost emails on the way in: the IMAP cursor only moves past mail that was actually stored (more than 50 new emails, a failed download or a crash between saving and queuing no longer drop mail); an email that cannot be fetched three times is reported in one combined IMAP alert per account per day instead of vanishing.
- Queued emails survive a restart (the message is fetched again by UID or Message-ID).
- After the mail server renumbers a mailbox (UIDVALIDITY change), already-notified emails are not sent again and new mail is not mistaken for old.
- Exactly one Telegram notification per email across retries, timeouts, restarts and the fallback path; if Telegram accepted a message but the reply was lost, it is never re-sent and is shown as "unconfirmed delivery" in /status and on the web health page.
- A second running copy of Letterbot refuses to start (it would notify every email twice); restore also refuses while Letterbot runs.
- Telegram rate limits (429 retry_after) are honoured; a rejected oversized message gets one minimal fallback notification; long texts are capped at Telegram's 4096-character limit; notifications list the first 2 attachments plus "… and N more".
- An email whose enrichment produced nothing is still notified (it previously could be dropped silently).
- Notification SLA alerts are actually delivered (they were silently skipped); IMAP dates are timezone-correct; the snooze reminder no longer says "yesterday" for a snooze made minutes ago around midnight.

- Each Telegram chat now sees only its own mailboxes in /status, /week, /stats and /commitments; snooze reminders go to the chat of the email's mailbox; /digest off applies per chat.
- Telegram output follows the closed emoji list everywhere (priority buttons are now 🔴🟡🔵); a guard at the send boundary removes anything else.
- An email can no longer be lost between processing steps (stage changes are atomic); a long IMAP outage right after a restart no longer fails queued emails.
- The packaged app reports "web UI ready" only once the dashboard is actually listening, and explains a busy port.

### Performance
- IMAP headers are fetched in batches with one logout per poll and a per-connection timeout; text clean-up is linear in length (a long Russian email no longer takes ~25 s).
- Text normalization is memoized (bounded) and the per-email metrics snapshot is reused within a poll cycle, with a new `decision_traces(created_at)` index; duplicate indexes on `emails(priority)` and `events_v1(event_type, ts_utc)` are dropped.
- Web cache is bounded, discarded dashboard queries are removed, and hidden browser tabs stop polling.

### Security
- Telegram bot tokens are redacted from logs (including tracebacks), stored delivery errors, failure notices and the diagnostics zip.
- Web archive and dashboard no longer expose raw email subjects or sender addresses; reflected XSS on the relationships pages fixed.
- Web UI rejects unknown `Host` headers (DNS rebinding); the session secret is random per process when not configured; the calibration report opens the database read-only.
- The dashboard asks for a password when opened from another computer (no password needed on the computer running Letterbot); without a configured password, network access is refused; repeated wrong passwords are throttled.
- Every dashboard page sends a nonce-based Content-Security-Policy and anti-framing headers; the login redirect accepts only same-site paths; the diagnostics zip also masks addresses in the bundled config and health files; logs no longer contain email subjects or senders.

## [28.0.0-rc] - 2026-03-21

### Added
- Release-candidate packaging and version surface synchronization

### Changed
- Version bumped from 27.2.0 to 28.0.0-rc for release candidate packaging
- Release metadata synchronized across active version surfaces

### Notes
- No new end-user features in this release candidate
- Functional scope matches 27.2.0 feature-complete baseline

## [28.0.0-rc.2] - 2026-02-28

### Added
- Deterministic Windows version-resource generator (`build/windows_version_info.txt`) tied to `mailbot_v26.version.get_version()`.

### Changed
- Windows one-folder artifact converged to Letterbot contract: `dist/Letterbot`, `Letterbot.exe`, `dist/Letterbot.zip`, and CI artifact `Letterbot-windows-onefolder`.
- PyInstaller entrypoint aligned to `mailbot_v26/__main__.py`; bundled config assets aligned to 2-file mode (`settings.ini.example` + `accounts.ini.example`).
- Dist launcher (`run_dist.bat`) switched to 2-file onboarding with `config-ready` retry gate and warning-first doctor flow.
- Release/Windows docs synchronized for current new-install flow (`settings.ini` + `accounts.ini`).

### Fixed
- Manifest integrity checks now ignore expected runtime-mutable files (`mailbot.log`, sqlite WAL/SHM, state/config runtime files) while still detecting unexpected extras/tampering.

### Security
- None.

## [28.0.0-rc.1] - 2026-02-28

### Added
- Release packaging pass: unified version surfaces (`__version__` + helper), `RELEASE_ARTIFACT.md`, `MANIFEST.json`, and compact RC smoke suite.

### Changed
- Windows `update_and_run.bat` diagnostics improved (Python >=3.10 check, pip check, explicit venv/log path, summary OK/FAIL).

### Fixed
- None.

### Security
- None.

## [27.2.0] - 2026-02-28

### Added
- Weekly digest support footer and `/status` Insider badge flow.
- Telegram snooze flow with reminders, plus `/commitments`, `/tasks`, and `/week` commands.
- Attachment insight line in Telegram payload.
- Cockpit contacts cards: top traffic, silent contacts, stalled dialogs.
- trust_bootstrap 2-file loader priority (`settings.ini` then legacy `config.ini`).

### Changed
- Weekly accuracy gate now requires `priority_corrections >= 3` and `accuracy_pct >= 80` in scheduler and weekly render.
- Preview actions are rendered as inline Telegram hint (`💡 ...`) behind trust gate (`>=10` corrections), without extra user-visible messages.
- trust_bootstrap template thresholds lowered to `min_samples = 10` and `templates_min_corrections = 10`.

### Fixed
- Windows `update_and_run.bat` now supports first-run/repair by creating `.venv` when missing and validating python availability.

### Security
- None.

## [27.1.0] - 2026-02-16

### Added
- Events-v1 source of truth for analytics/digests.
- SystemOrchestrator mode snapshots and policy logging.
- Trust v2 decay/redemption with versioned snapshots.
- Integration degradation tests A-G and single-cycle harness.
- GigaChat global lock (single in-flight request).
- Doctor diagnostics mode and CLI `--version` support.
- Windows bootstrap scripts standardized for `.venv` and one-command workflows.

### Changed
- Release process hardened for deterministic Windows one-folder artifact flow.

### Fixed
- None.

### Security
- None.
