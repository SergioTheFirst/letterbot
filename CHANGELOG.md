# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- None.

### Changed
- None.

### Fixed
- None.

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
