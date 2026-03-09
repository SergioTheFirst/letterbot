# Golden Corpus

- Synthetic and sanitized only. No real customer data, secrets, or message IDs.
- Each case is evaluated through the canonical processor flow:
  `facts -> validation -> scoring -> consistency -> template layer -> decision -> interpretation`.
- `cases.json` is the deterministic offline baseline used by:
  - `python -m mailbot_v26.tools.eval_golden_corpus`
  - regression tests for critical document classes
- `dry_run_validated` corpus cases also execute the offline `.eml` harness and
  assert the final Telegram render contract:
  - `eml_fixture`
  - `expected_render_mode`
  - `expected_render_contains`
  - `expected_render_not_contains`
- `.eml` fixtures live under `mailbot_v26/tests/fixtures/eml/` and are
  synthetic counterparts for the end-to-end dry-run gate.
- The current corpus contains 138 synthetic cases across:
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
  - `e2e_dry_run`
- Template-promotion analysis is still deterministic and canonical-event-based. Runtime auto-learning is not enabled here; promotion helpers are quality/reliability infrastructure only.
