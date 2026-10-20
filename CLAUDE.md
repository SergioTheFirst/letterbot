# Letterbot

Letterbot (LetterBot.ru) is a local, Windows-first layer over e-mail: it parses incoming mail, enriches it with an LLM and sends one Telegram notification per email, plus a read-only Flask web cockpit. Python 3.10, SQLite, no Docker. The package is `mailbot_v26/` (legacy name, kept on purpose).

## Sources of truth
- `CONSTITUTION.md` (Russian, binding) outranks every other instruction here. If a request conflicts with it or with `CONTINUITY.md`, stop, explain the conflict, offer 1-2 alternatives and wait for approval.
- `CONTINUITY.md` holds the current Goal / Constraints / State / Now / Next; read it at task start. Older history lives in `docs/CONTINUITY_ARCHIVE.md`; search it only when you need history.
- Read the matching CONSTITUTION section before changing Telegram output, LLM usage, events/privacy, the web UI, heuristics/thresholds or ingest/runtime behaviour. Its non-negotiables in short:
  - LLMs: only Cloudflare Workers AI and GigaChat, given only the minimum context they need. GigaChat runs one request at a time under the global lock; on busy/error fall back to Cloudflare or degraded mode without blocking the pipeline. No paid services.
  - A plain Windows PC must be enough; external services only enhance - degrade, never crash.
  - User data stays local (SQLite, exportable). Raw emails and attachments never go to the cloud; events never store raw body, subject or Telegram text; logs and telemetry keep PII local.
  - Telegram: one email -> one notification. Tier-1 never waits on slow extraction/LLM past `MAX_TELEGRAM_WAIT_SECONDS` (then a minimal Tier-1 goes out once); later enrichment only by edit-in-place; no batching or deferral. Tier-1 = facts -> conclusion -> action, at most 15 lines, emoji only from Appendix A.
  - The web UI is read-only and shows sanitized aggregates only - never raw email text, subjects or senders. Single exception (CONSTITUTION IV, rev. 2026-10): the loopback-only Settings page may write config files (accounts.ini, settings.ini), never the DB; secrets are write-only and never shown, logged or exported; saves are validated, atomic, backed up and recorded as a secret-free event.
  - Heuristic and threshold changes go through shadow validation, metrics and a rollback path.
  - No heavy dependencies (no LibreOffice); strict IMAP UID idempotency; ingest only mail that arrives after process start unless a maintenance backfill flag is set.

## Repo map
- `mailbot_v26/`: `pipeline/`, `telegram/`, `llm/`, `storage/`, `events/`, `web_observability/` (Flask + Jinja templates), `tools/` (CLIs), `config/*.example` (config templates).
- Tests: `mailbot_v26/tests/` and `tests/` (both listed in `pytest.ini`).
- Launch: `letterbot.bat` (Windows source launcher), `python -m mailbot_v26`, `python -m mailbot_v26.tools.run_stack` (worker + web). Packaging: `build_windows_onefolder.bat` -> `dist/Letterbot/` and the installer.
- Credentials: `accounts.ini`, `settings.ini`, `keys.ini` (gitignored) - never print or commit them.
- Runtime and build output: `database.sqlite` (~200 MB), `logs/`, `data/`, `runtime/`, `dist/`, `build/`, `.venv/` - open them only when the task is about them. `build/pyinstaller/` is still tracked, so stage it only for packaging work.

## Commands (same gates as CI)
- `python -m compileall mailbot_v26 -q`
- While iterating: `python -m pytest -q --tb=short -x <the test files for the area you touched>`
- Full suite (~5 min): `python -m pytest -q --tb=short`, once per finished change set; keep only the summary and failures (`| tail -n 30` in Bash, `| Select-Object -Last 30` in PowerShell).
- `python -m mailbot_v26.tools.eval_golden_corpus` (`--report` for per-category detail)
- `python -m mailbot_v26.tools.cleanup --status`

## Changing code
- Minimal, targeted diffs; no drive-by refactors; keep the pipeline stage order (PARSE -> LLM -> TG).
- Change the runtime path, or put the change behind a flag that actually routes runtime; don't improve dead code paths.
- `pipeline/processor.py`, `web_observability/app.py` and `storage/analytics.py` are 7k+ lines each: grep for the symbol and read line ranges; leave their size alone unless the task is a split.
- Deterministic rules over new LLM calls. No new dependencies without approval.
- Never swallow errors (`except: pass` and the like): log them and surface the degraded result to the user.
- Make every new side effect observable (log line, metric or event) without putting raw email content in it.
- A behaviour change ships with added or adjusted tests. Telegram rendering is locked by snapshot fixtures in `mailbot_v26/tests/fixtures/telegram_snapshots/` (asserted by `test_telegram_render.py`); update them deliberately.
- Keep `.bat` files ASCII-only (cmd.exe code pages); `test_letterbot_bat_is_ascii_safe` in `mailbot_v26/tests/test_windows_launcher_contract.py` guards `letterbot.bat`.
- Branding: user-visible text uses the Letterbot / LetterBot.ru brand (see CONTINUITY State); don't rename packages, modules, paths or config keys unless asked.
- Review the finished change set once, scoped to its diff, before handing off - not after every edit.

## Keeping CONTINUITY.md current
At the end of a change series insert one short line directly under `Done:` (newest first: date, what changed, how it was verified). Update `Now`/`Next`, and `State` / `Key decisions` / `Constraints` when a decision or invariant changes. Keep the section order (Goal, Constraints / Assumptions, Key decisions, State, Done, Now, Next, Open questions, Working set). Keep `Done:` to about the last 10 entries: move older ones verbatim to the top of the list in `docs/CONTINUITY_ARCHIVE.md`. UTF-8, facts only, no secrets; mark unknowns `UNCONFIRMED`.
