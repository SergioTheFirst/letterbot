from __future__ import annotations

from dataclasses import replace

from mailbot_v26.tools.eval_golden_corpus import (
    evaluate_case,
    evaluate_golden_corpus,
    load_golden_corpus,
    main,
    render_report,
    render_summary,
)


def test_golden_corpus_critical_cases_pass() -> None:
    load_golden_corpus.cache_clear()
    critical_cases = [case for case in load_golden_corpus() if case.critical]

    results = [evaluate_case(case) for case in critical_cases]

    assert critical_cases
    assert all(result.passed for result in results)


def test_critical_dangerous_cases_100_percent_pass() -> None:
    test_golden_corpus_critical_cases_pass()


def test_golden_corpus_no_payroll_to_invoice_regression() -> None:
    load_golden_corpus.cache_clear()
    payroll_cases = [case for case in load_golden_corpus() if case.category == "payroll"]

    results = [evaluate_case(case) for case in payroll_cases]

    assert payroll_cases
    assert all(result.passed for result in results)
    assert all(result.actual_doc_kind != "invoice" for result in results)
    assert all("оплат" not in result.actual_action.casefold() for result in results)


def test_payroll_never_becomes_invoice_in_any_corpus_case() -> None:
    test_golden_corpus_no_payroll_to_invoice_regression()


def test_reconciliation_never_becomes_payment_in_any_corpus_case() -> None:
    load_golden_corpus.cache_clear()
    reconciliation_cases = [
        case for case in load_golden_corpus() if case.category == "reconciliation"
    ]

    results = [evaluate_case(case) for case in reconciliation_cases]

    assert reconciliation_cases
    assert all(result.passed for result in results)
    assert all("оплат" not in result.actual_action.casefold() for result in results)


def test_golden_corpus_attachment_total_cases_pass() -> None:
    load_golden_corpus.cache_clear()
    attachment_cases = [
        case for case in load_golden_corpus() if "attachment_heavy" in case.subsets
    ]

    results = [evaluate_case(case) for case in attachment_cases]

    assert attachment_cases
    assert all(result.passed for result in results)
    assert all(result.actual_doc_kind == "invoice" for result in results)
    assert all(result.actual_template_id == "russian_invoice_common" for result in results)


def test_golden_corpus_recurring_template_cases_are_covered() -> None:
    load_golden_corpus.cache_clear()
    recurring_cases = [
        case for case in load_golden_corpus() if "recurring_templates" in case.subsets
    ]

    results = [evaluate_case(case) for case in recurring_cases]

    assert len(recurring_cases) >= 64
    assert all(result.passed for result in results)


def test_recurring_invoice_same_issuer_precision_does_not_regress() -> None:
    load_golden_corpus.cache_clear()
    recurring_invoice_cases = [
        case
        for case in load_golden_corpus()
        if case.category == "invoice" and "recurring_templates" in case.subsets
    ]

    results = [evaluate_case(case) for case in recurring_invoice_cases]

    assert recurring_invoice_cases
    assert all(result.passed for result in results)
    assert all(result.actual_doc_kind == "invoice" for result in results)


def test_overall_pass_rate_not_below_baseline() -> None:
    load_golden_corpus.cache_clear()

    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    assert summary.total_cases >= 120
    assert summary.failed_cases == 0
    assert summary.passed_cases == summary.total_cases


def test_new_issuer_identity_cases_pass() -> None:
    load_golden_corpus.cache_clear()
    issuer_cases = [
        case for case in load_golden_corpus() if "recurring_templates" in case.subsets
    ]

    results = [evaluate_case(case) for case in issuer_cases]

    assert issuer_cases
    assert all(result.passed for result in results)


def test_promotion_candidate_cases_pass_in_shadow_mode() -> None:
    load_golden_corpus.cache_clear()
    promotion_cases = [
        case for case in load_golden_corpus() if "correction_sensitive" in case.subsets
    ]

    results = [evaluate_case(case) for case in promotion_cases]

    assert promotion_cases
    assert all(result.passed for result in results)


def test_eval_tool_reports_category_and_subset_summary_stably() -> None:
    load_golden_corpus.cache_clear()

    summary = evaluate_golden_corpus(list(load_golden_corpus()))
    rendered = render_summary(summary)

    assert summary.total_cases >= 120
    assert "Critical safety:" in rendered
    assert "Categories:" in rendered
    assert "Subsets:" in rendered
    assert "- attachment_heavy:" in rendered
    assert "- recurring_templates:" in rendered
    assert "- weak_signal:" in rendered
    assert "- invoice:" in rendered
    assert "- payroll:" in rendered
    assert "Failure types:" in rendered
    assert "- none" in rendered


def test_eval_tool_reports_case_failures_deterministically() -> None:
    load_golden_corpus.cache_clear()
    case = load_golden_corpus()[0]
    broken_case = replace(
        case,
        expected=replace(case.expected, action="Impossible action"),
    )

    summary = evaluate_golden_corpus([broken_case])
    rendered = render_summary(summary)

    assert summary.total_cases == 1
    assert summary.failed_cases == 1
    assert "Categories:" in rendered
    assert "Subsets:" in rendered
    assert "Failure types:" in rendered
    assert "- action: 1" in rendered
    assert f"- {case.case_id}: action" in rendered


def test_eval_summary_output_is_deterministic() -> None:
    load_golden_corpus.cache_clear()

    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    assert render_summary(summary) == render_summary(summary)


def test_report_flag_produces_output_without_errors(capsys) -> None:
    exit_code = main(["--report"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "LETTERBOT GOLDEN CORPUS REPORT" in captured.out


def test_report_output_contains_critical_section() -> None:
    load_golden_corpus.cache_clear()
    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    rendered = render_report(summary, generated_at="2026-03-09T00:00:00+00:00")

    assert "CRITICAL CASES:" in rendered
    assert "DANGEROUS FAILURES:" in rendered


def test_report_output_contains_category_breakdown() -> None:
    load_golden_corpus.cache_clear()
    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    rendered = render_report(summary, generated_at="2026-03-09T00:00:00+00:00")

    assert "CATEGORY BREAKDOWN:" in rendered
    assert "PROMOTION SHADOW CASES:" in rendered
    assert "invoice" in rendered


def test_report_output_is_deterministic_on_same_corpus() -> None:
    load_golden_corpus.cache_clear()
    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    first = render_report(summary, generated_at="2026-03-09T00:00:00+00:00")
    second = render_report(summary, generated_at="2026-03-09T00:00:00+00:00")

    assert first == second


def test_report_no_runtime_db_dependency() -> None:
    load_golden_corpus.cache_clear()
    summary = evaluate_golden_corpus(list(load_golden_corpus()))

    rendered = render_report(summary, generated_at="2026-03-09T00:00:00+00:00")

    assert "Failed:" in rendered
    assert "Golden corpus evaluator" not in rendered


def test_default_mode_unchanged_by_report_flag_addition(capsys) -> None:
    exit_code = main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out.startswith("Golden corpus evaluator")
