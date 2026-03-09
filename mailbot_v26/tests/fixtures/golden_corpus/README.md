# Golden Corpus

- Synthetic and sanitized only. No real customer data, secrets, or message IDs.
- Each case is evaluated through the canonical processor flow:
  `facts -> validation -> scoring -> consistency -> template layer -> decision -> interpretation`.
- `cases.json` is the deterministic offline baseline used by:
  - `python -m mailbot_v26.tools.eval_golden_corpus`
  - regression tests for critical document classes
- The current corpus contains 128 synthetic cases across:
  - invoice
  - payroll
  - reconciliation
  - contract/amendment
  - generic notification
  - table-heavy attachment
  - noisy PDF-like attachment with repeated headers
  - reply/forward polluted email
  - sender ambiguous but content clear
  - sender clear but content weak
- Cases are also tagged by deterministic subsets:
  - `critical_safety`
  - `recurring_templates`
  - `attachment_heavy`
  - `weak_signal`
  - `correction_sensitive`
  - `digest_projection_sensitive`
- Template-promotion analysis is still deterministic and canonical-event-based. Runtime auto-learning is not enabled here; promotion helpers are quality/reliability infrastructure only.
