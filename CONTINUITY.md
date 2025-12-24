Goal (incl. success criteria):
- Fix Telegram bot message formatting so forwarded email content renders cleanly (no raw headers/\n) and all related functions work as expected. Success: Telegram output shows readable sender/subject/body without raw escape sequences; features verified.

Constraints/Assumptions:
- Must read/update this ledger each turn.
- Follow repo AGENTS instructions; commit changes and create PR after.
- Use short, factual bullets; mark unknowns UNCONFIRMED.

Key decisions:
- Preserve <b>/<i> tags in telegram_safe while sanitizing summaries to avoid raw HTML.
- Keep MessageProcessor output structure but improve summary/attachments generation.

State:
- Code changes complete; tests run.

Done:
- Added cleaned body summary and attachment summary logic in MessageProcessor.
- Allowed Telegram-safe HTML tags (<b>/<i>) in telegram_safe.
- Ran targeted pytest subset.

Now:
- Prepare commit and PR.

Next:
- Commit changes and create PR.

Open questions (UNCONFIRMED if needed):
- Confirm if Telegram template should be further adjusted beyond current structure.

Working set (files/ids/commands):
- mailbot_v26/pipeline/processor.py
- mailbot_v26/telegram_utils.py
- pytest mailbot_v26/tests/test_body_summary.py mailbot_v26/tests/test_pipeline_processor.py mailbot_v26/tests/test_attachment_descriptions.py
