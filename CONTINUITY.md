Goal (incl. success criteria):
- Standardize Telegram HTML renderer v1.0 with unified layout, escaping, and attachment truncation; success criteria: formatter helpers wired in processor and tests updated.

Constraints / Assumptions:
- Follow repo AGENTS instructions; update this ledger each turn.
- No LLM logic or pipeline order changes; no preview/digest changes; HTML only.
- Failures must be logged and must not interrupt mail processing.

Key decisions:
- Introduced tg_formatter helper functions for header/subject/action/attachments.
- Escaped dynamic fields before HTML tags via escape_tg_html.
- Attachment snippets truncated with "...." suffix.

State:
- Unified renderer wired into processor with safe plain-text fallback.
- Tests updated and new formatting coverage added.

Done:
- Implemented tg_formatter and updated processor rendering/fallback behavior.
- Added formatting tests and updated Telegram payload expectations.

Now:
- Unified HTML renderer v1.0 implemented and verified in tests.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files / tables / tests):
- mailbot_v26/pipeline/processor.py
- mailbot_v26/pipeline/tg_formatter.py
- mailbot_v26/tests/test_telegram_payload_validation.py
- mailbot_v26/tests/test_telegram_payload_pipeline.py
- mailbot_v26/tests/test_telegram_render_modes.py
- mailbot_v26/tests/test_insight_aggregator_pipeline.py
- mailbot_v26/tests/test_priority_confidence.py
- mailbot_v26/tests/test_relationship_health_pipeline.py
- mailbot_v26/tests/test_trust_score_pipeline.py
- mailbot_v26/tests/test_telegram_rendering_format.py
