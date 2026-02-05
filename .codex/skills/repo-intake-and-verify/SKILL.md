---
name: repo-intake-and-verify
description: "Repository intake and verification checklist. Use at the start of ANY task before editing code: read AGENTS/CONSTITUTION/CONTINUITY, inventory key files, identify entrypoints, run compileall+pytest, then propose a 3–6 step plan."
---

1) Read AGENTS.md, CONSTITUTION.md, and CONTINUITY.md.
2) Print repo root and a limited file inventory.
3) Identify impacted modules and entrypoint(s).
4) Run `python -m compileall mailbot_v26` and `pytest -q` (or explain why not possible).
5) Write a short 3–6 step plan; only then modify code.
6) End the response with: Summary / Testing / Changed files / CONTINUITY update (if relevant).
