# AGENTS.md — Letterbot (Codex instructions)

This file is the single operational instruction set for Codex in this repo.

Codex MUST:
- follow these rules strictly;
- keep diffs small and targeted;
- prefer deterministic logic over heuristic/LLM dependency;
- update CONTINUITY.md whenever state materially changes (Done/Now/Next).

If any instruction here conflicts with CONSTITUTION.md, CONSTITUTION.md wins.

---

## 1) Sources of truth (priority order)
1) CONSTITUTION.md — purpose, hard constraints, non-negotiables.
2) CONTINUITY.md — what is true right now: Goal / State / Done / Now / Next.
3) AGENTS.md — how to work in this repo (this file).

If something is not in CONSTITUTION.md → it is NOT a principle.
If something is not in CONTINUITY.md → it is UNKNOWN. Mark as UNCONFIRMED, do not invent context.

---

## 2) Mandatory start-of-turn routine (ALWAYS)
Before any coding:
1) Read CONSTITUTION.md and CONTINUITY.md.
2) Identify the minimal working set (files/tests touched).
3) Make a 3–5 step plan.
4) Define verification (tests/commands).
5) Only then implement.

If CONTINUITY.md is missing:
- Create it in repo root using the required format (section order is strict).
- Fill only facts you can prove from repo state. Unknowns → UNCONFIRMED.

---

## 3) Repo quick map (update if structure changes)
- Python project.
- Primary package directory: `mailbot_v26/` (legacy naming; product name is "Letterbot").
- Config templates: `mailbot_v26/config/`
- Tests: `mailbot_v26/tests/` (pytest)

When renaming user-facing text/branding:
- Do NOT rename modules/packages unless explicitly requested.
- Prefer changing UI strings, headings, CLI help, templates, README, and visible labels.

---

## 4) How to run (commands Codex should use)
Environment assumptions:
- Windows PowerShell or cmd is common; avoid bash-only commands unless confirmed.

Preferred commands:
- Run tests: `python -m pytest -q`
- Run a specific test: `python -m pytest -q path\\to\\test_file.py -k test_name`
- Lint/format: only if tools are present in repo (check `pyproject.toml` / `requirements*.txt` first).

If commands are unknown:
- Inspect repo files to confirm (pyproject.toml, requirements, README).
- If still unclear, propose 1–2 options and ask user to confirm.

---

## 5) Development rules (NON-NEGOTIABLE)
### Change strategy
- Minimal diffs. No refactors "just because".
- Preserve pipeline stage order unless explicitly required.
- No silent error handling. Never `except: pass`. Always log + surface degraded output.

### Determinism first
- Prefer rules/heuristics over new LLM calls or probabilistic logic.
- Do not add new external dependencies unless necessary and approved.

### Safety & observability
- Any new side effect must be observable (logs / metrics / explicit events).
- If output quality degrades, say so explicitly in user-facing output.

### Tests (TDD bias)
- For behavior changes: add/adjust tests in the same PR.
- Keep tests fast and deterministic.

---

## 6) CONTINUITY.md update policy (MANDATORY)
Update CONTINUITY.md immediately when:
- Goal or success criteria changes,
- a phase is completed,
- new invariant constraints appear,
- a feature is fully implemented & tested,
- tests added/removed that affect architecture,
- a long-term decision is made.

Rules:
- short, factual bullets
- no chat logs, no prose
- unknowns → UNCONFIRMED
- never store secrets/credentials

Required section order in CONTINUITY.md:
Goal (incl. success criteria):
Constraints / Assumptions:
Key decisions:
State:
Done:
Now:
Next:
Open questions (UNCONFIRMED if needed):
Working set (files / tables / tests):

---

## 7) Communication format (responses Codex should produce)
Be concise and implementation-oriented.
No motivational filler. No invented facts.

Use this template:

Ledger Snapshot:
- Goal: (from CONTINUITY.md)
- Now:
- Next:
- Blockers:

Then:
- What will change (files)
- Exact commands to verify
- Any risks

---

## 8) Escalation rule (CRITICAL)
If there is a conflict between:
- user request,
- CONSTITUTION.md,
- CONTINUITY.md,
- architecture constraints,

Codex MUST stop and:
1) explain the conflict clearly;
2) propose 1–2 alternatives with pros/cons;
3) wait for explicit approval.

Silent compromise is forbidden.

---

## 9) Common tasks playbooks
### Rename "mailbot" → "letterbot" in UI/branding
- Search for user-visible strings: "mailbot", "MailBot", "MAILBOT"
- Update:
  - UI templates / web pages (if present)
  - CLI help text / output strings
  - README/docs
- DO NOT rename python packages or folder names unless requested.
- Add/adjust tests if output is asserted.

---

ACTIVATION PHRASE:
Ready. What's the first task from CONTINUITY.md?
