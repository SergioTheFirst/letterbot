Goal (incl. success criteria):
- Add a deterministic Message Insight Arbiter between LLM and Telegram send that replaces vague summaries with clear rule-based fallbacks, without changing Telegram payload schema or pipeline order. Success: new arbiter behavior is covered for attachments-only, commitments present, and extraction failure cases; TG payload stability tests still pass; system continues if arbiter fails.

Constraints/Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- Do not change Telegram payload structure, pipeline order, queue, IMAP, or LLM logic.
- Arbiter is pure rule-based, idempotent, side-effect free; log decisions with [INSIGHT-ARBITER].
- Commit changes and create PR after.

Key decisions:
- Insert a deterministic arbiter step after LLM summary and before Telegram send; if arbiter fails, fall back to existing behavior.
- Define low-signal detection using rule-based heuristics over LLM summary and extracted metadata.

State:
- Implemented Message Insight Arbiter and tests.

Done:
- Added deterministic Message Insight Arbiter with low-signal detection and fallbacks.
- Wired arbiter into pipeline before Telegram payload build with safe failure handling.
- Added unit tests for arbiter (attachments-only, commitments, extraction failure).
- Tests: pytest mailbot_v26/tests/test_insight_arbiter.py; pytest mailbot_v26/tests/test_observability_logging.py::test_telegram_payload_stability.

Now:
- Review changes, commit, and prepare PR.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- mailbot_v26/pipeline/insight_arbiter.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/tests/test_insight_arbiter.py
- pytest mailbot_v26/tests/test_insight_arbiter.py
- pytest mailbot_v26/tests/test_observability_logging.py::test_telegram_payload_stability
