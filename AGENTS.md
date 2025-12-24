# Codex Workspace Operating Rules (binding)

These rules are mandatory unless the user explicitly overrides them.

## 0) Canonical source of truth
- The canonical, compaction-safe workspace ledger is: `constitution.md` (repo root unless stated otherwise).
- Treat `constitution.md` as the only trusted memory across long sessions.
- If something is not written in `constitution.md`, treat it as UNKNOWN and ask (at most) 1–3 targeted questions.

## 1) Mandatory start-of-turn routine (ALWAYS)
Before doing any work (planning, coding, running commands):
1) Open and read `constitution.md`.
2) Update it if needed (see Section 2).
3) Only then proceed.

If `constitution.md` does not exist:
- Create it in the repository root with the template in Section 4, then continue.

## 2) When to update `constitution.md` (ALWAYS)
Update `constitution.md` immediately when any of the following changes:
- Goal or success criteria
- Constraints / assumptions
- Key decisions / tradeoffs
- Current progress state
- Important tool outcomes (tests, builds, migrations, deployments, benchmarks)

Update discipline:
- Keep it SHORT, factual, stable.
- Use bullets.
- Mark unknowns as `UNCONFIRMED` (do not guess).
- Do NOT paste chat transcripts.
- Do NOT include secrets or tokens.

## 3) Execution behavior
- Prefer concrete, verifiable actions over speculation.
- When editing code: keep changes minimal, aligned with repo conventions.
- Run the most relevant checks/tests after changes when feasible.
- If a step is risky or irreversible, ask for approval first.

## 4) Required `constitution.md` format (keep headings)
Use exactly these headings:

- Goal (incl. success criteria):
- Constraints/Assumptions:
- Key decisions:
- State:
- Done:
- Now:
- Next:
- Open questions (UNCONFIRMED if needed):
- Working set (files/ids/commands):

## 5) Communication
- In chat responses: be brief and action-oriented.
- Do NOT add ritual preambles.
- Only include a “Constitution Snapshot” (Goal + Now/Next + Open Questions) when:
  a) the user asks, or
  b) you made a material update to `constitution.md`.
