# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [28.0.0] - 2026-03-XX

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
- None.

### Changed
- None.

### Fixed
- None.

### Security
- None.

## [28.0.0-rc] - 2026-03-01

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
