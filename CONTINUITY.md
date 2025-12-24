Goal (incl. success criteria):
- Add deterministic Attention Gate immediately before Telegram send to defer non-urgent emails to Digest, without changing pipeline order or Telegram payload schema. Success: gate logs decisions, defers attachments-only informational emails with persistence, bypasses high priority or commitments, defaults to current behavior on gate failure, and existing Telegram payload stability tests still pass.

Constraints/Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- Do not change Telegram payload structure, pipeline order, queue, IMAP, or LLM logic.
- Attention Gate is pure rule-based; log decisions with [ATTENTION-GATE].
- System remains operational if gate errors (fallback to current behavior).
- Commit changes and create PR after.

Key decisions:
- Insert deterministic gate immediately before Telegram send; on error, fall back to send.
- Gate decisions set deferred_for_digest=true and persist for later digest when gated.

State:
- Attention Gate implemented with persistence flag and tests added.

Done:
- Added deterministic Attention Gate before Telegram send with safe fallback.
- Persisted deferred_for_digest flag in CRM storage with migrations.
- Added tests for gate logic, persistence, and payload stability.

Now:
- Commit changes and prepare PR.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- mailbot_v26/pipeline/attention_gate.py
- mailbot_v26/pipeline/processor.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/storage/schema.sql
- mailbot_v26/tests/test_attention_gate.py
- mailbot_v26/tests/test_attention_gate_persistence.py
- pytest mailbot_v26/tests/test_attention_gate.py
- pytest mailbot_v26/tests/test_attention_gate_persistence.py
- pytest mailbot_v26/tests/test_observability_logging.py::test_telegram_payload_stability
