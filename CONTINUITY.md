Goal (incl. success criteria):
- Apply stability hotfixes: decompose process_message safely, guard oversize/zip-bomb/extraction caps, and harden SQLite writes (busy handling) with tests; ensure Telegram payload stability remains green.

Constraints / Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- Do not change Telegram payload structure for normal emails, pipeline order, queue, IMAP, or LLM logic.
- Failures must be logged and must not interrupt mail processing.

Key decisions:
- IMAP oversize guard uses RFC822.SIZE to skip bodies and injects a warning body for the pipeline.
- Attachment extraction is capped and guarded by zip uncompressed size checks before extraction.
- KnowledgeDB writes use a locked, retrying write_transaction with busy_timeout set per connection.

State:
- Stability hotfixes implemented for IMAP oversize handling, attachment caps/zip guard, and SQLite busy handling.

Done:
- Decomposed process_message into context, analytics, and telegram render helpers with side-effect guards.
- Added IMAP oversize handling and attachment extraction caps with zip-bomb guard.
- Added KnowledgeDB write_transaction with retry/backoff and busy_timeout, plus tests for oversize and busy handling.

Now:
- Stability hotfixes implemented and tested.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/processor.py
- mailbot_v26/imap_client.py
- mailbot_v26/bot_core/pipeline.py
- mailbot_v26/storage/knowledge_db.py
- mailbot_v26/config_loader.py
- mailbot_v26/config/config.ini
- mailbot_v26/tests/test_oversize_email.py
- mailbot_v26/tests/test_knowledge_db_busy.py
