---
name: tg-no-duplicates
description: "Enforce Telegram message clarity: one sentence = one thought, dedup sentences within fields and dedup lines across sections. Use whenever changing tg_renderer, processor Telegram text assembly, or Telegram formatting."
---

1) Treat mailbot_v26/pipeline/tg_renderer.py and mailbot_v26/pipeline/processor.py Telegram builders as canonical modules.
2) Enforce sentence splitting and semantic duplicate removal within each field.
3) Apply a final line-level dedup safeguard on rendered Telegram text.
4) Require golden tests covering duplicates and formatting changes.
5) Do not introduce new NLP dependencies.
