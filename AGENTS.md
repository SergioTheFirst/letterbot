# Codex Workspace Operating Rules (Binding)

These rules define how Codex (or any automated agent) must operate inside this repository.
They are mandatory unless the user explicitly overrides them.

This project is governed by two canonical documents:
- CONSTITUTION.md — strategic principles and long-term direction
- CONTINUITY.md — operational state and short-term memory

Confusing their roles is a critical error.

---

## 0) Canonical sources of truth (STRICT)

### Strategic truth
- `CONSTITUTION.md` defines:
  - what kind of system this is
  - what is allowed / forbidden
  - long-term goals and philosophy
- It changes rarely and intentionally.
- Codex MUST respect it at all times.

### Operational truth
- `CONTINUITY.md` defines:
  - current goal
  - current state of implementation
  - what is done / now / next
- It is NOT a roadmap and NOT a feature wishlist.
- It is a ledger of facts, not intentions.

If something is not written in:
- CONSTITUTION.md → it is not a principle
- CONTINUITY.md → it is UNKNOWN

Do not invent missing context.

---

## 1) Mandatory start-of-turn routine (ALWAYS)

Before doing **any** work (analysis, planning, coding, refactoring, tests):

1) Open and read `CONSTITUTION.md`
2) Open and read `CONTINUITY.md`
3) Verify that the planned action:
   - does not violate the Constitution
   - aligns with the current Goal / Now / Next

Only after that may you proceed.

If `CONTINUITY.md` does not exist:
- Create it in the repository root using the required format (see Section 4)
- Populate it with **minimal factual state**
- Then continue

---

## 2) When to update `CONTINUITY.md` (MANDATORY)

Update `CONTINUITY.md` immediately when any of the following occur:

- Goal or success criteria changed
- A phase is completed (e.g. “Attention Gate implemented”)
- A new invariant or constraint is discovered
- A feature is fully implemented and tested
- Tests are added/removed with architectural meaning
- A decision with long-term impact is made

### Update discipline
- Keep it SHORT and factual
- Use bullet points
- No prose, no justifications
- No chat logs
- No speculation
- Mark unknowns explicitly as `UNCONFIRMED`
- Never store secrets or credentials

⚠️ `CONTINUITY.md` is **documentation of reality**, not a plan for the future.

---

## 3) Development behavior (NON-NEGOTIABLE)

- Prefer deterministic logic over probabilistic whenever possible
- Never introduce LLM dependency where rules suffice
- Any new layer must:
  - fail safely
  - not block mail processing
  - not change Telegram payload schema unless explicitly approved

### Code changes
- Minimal, surgical diffs
- Respect existing module boundaries
- No reordering of pipeline stages unless instructed
- Side-effects must be observable (logs, metrics)

### Error handling
- Errors are logged, never hidden
- A failure must not silently degrade user trust
- If user-facing output degrades, it must say so explicitly

---

## 4) Required `CONTINUITY.md` format (EXACT)

Use **exactly** these headings, in this order:

- Goal (incl. success criteria):
- Constraints / Assumptions:
- Key decisions:
- State:
- Done:
- Now:
- Next:
- Open questions (UNCONFIRMED if needed):
- Working set (files / tables / tests):

No additional sections.
No renaming.
No narrative text.

---

## 5) Relationship between documents (IMPORTANT)

- CONSTITUTION.md answers: **“Why this system exists and what it must become”**
- CONTINUITY.md answers: **“What is true right now”**
- agents.md answers: **“How Codex must behave”**

Codex must NEVER:
- treat CONTINUITY.md as a roadmap
- modify CONSTITUTION.md implicitly
- invent “Next” items without user or expert consensus

---

## 6) Communication rules

- Be brief, concrete, and implementation-oriented
- No ritual preambles
- No motivational language
- No emojis
- No speculation presented as fact

Include a **“Constitution Snapshot”** in chat responses ONLY when:
- the user explicitly asks for it, or
- you made a material change to CONTINUITY.md

---

## 7) Escalation rule (CRITICAL)

If you detect a conflict between:
- requested action
- CONSTITUTION.md
- existing architecture

You MUST:
- stop
- explain the conflict
- propose 1–2 alternatives
- wait for explicit approval

Silent compromise is forbidden.
