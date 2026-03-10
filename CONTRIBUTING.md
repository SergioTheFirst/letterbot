# Contributing to Letterbot

## Before you open a PR

- Keep diffs small and targeted.
- Do not add new dependencies without prior discussion.
- Preserve the canonical flow: facts -> validation -> scoring -> consistency -> template layer -> decision -> interpretation.
- Do not introduce regressions in dangerous payroll, reconciliation, or invoice cases.
- Do not commit local runtime files, databases, logs, or real credentials.

## Required verification

Run these commands locally:

- `python -m compileall mailbot_v26 -q`
- `python -m pytest mailbot_v26/tests/ -q --tb=short`
- `python -m mailbot_v26.tools.eval_golden_corpus`

If your change touches operational maintenance, also run:

- `python -m mailbot_v26.tools.cleanup --status`

## Configuration and secrets

- Start from the shipped `*.example` templates.
- Keep `settings.ini`, `accounts.ini`, `keys.ini`, `.env`, and any local overrides out of git.
- Replace any accidentally committed credential with a placeholder and rotate the real secret.

## Architecture guardrails

- `events_v1` and `message_interpretation` remain the semantic source of truth.
- Read models and cockpit views must stay rebuildable from canonical events.
- Cleanup and retention must use narrow allowlists, never broad destructive deletes.
