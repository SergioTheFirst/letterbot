"""Deterministic offline evaluator for the canonical golden corpus."""

from __future__ import annotations

import argparse
import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from mailbot_v26.pipeline import processor as pipeline_processor

DEFAULT_CORPUS_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "golden_corpus"
    / "cases.json"
)
DEFAULT_EML_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "eml"
)


@dataclass(frozen=True, slots=True)
class GoldenAttachment:
    filename: str
    text: str


@dataclass(frozen=True, slots=True)
class GoldenExpected:
    doc_kind: str | None
    amount: str | float | int | None
    due_date: str | None
    doc_number: str | None
    action: str
    priority: str | None = None
    template_id: str | None = None
    must_not_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GoldenCase:
    case_id: str
    category: str
    sender_email: str
    subject: str
    body_text: str
    attachments: tuple[GoldenAttachment, ...]
    expected: GoldenExpected
    critical: bool = False
    mail_type: str = ""
    subsets: tuple[str, ...] = ()
    dry_run_validated: bool = False
    eml_fixture: str | None = None
    expected_render_mode: str | None = None
    expected_render_contains: tuple[str, ...] = ()
    expected_render_not_contains: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GoldenBucketSummary:
    name: str
    total: int
    passed: int
    failed: int


@dataclass(frozen=True, slots=True)
class GoldenFailureSummary:
    name: str
    count: int


@dataclass(frozen=True, slots=True)
class GoldenCaseResult:
    case: GoldenCase
    doc_kind_ok: bool
    amount_exact_ok: bool
    amount_tolerant_ok: bool
    due_date_ok: bool
    doc_number_ok: bool
    action_ok: bool
    priority_ok: bool
    template_ok: bool
    forbidden_flags_ok: bool
    dry_run_ok: bool
    render_mode_ok: bool
    render_contains_ok: bool
    render_not_contains_ok: bool
    passed: bool
    actual_doc_kind: str | None
    actual_amount: float | None
    actual_due_date: str | None
    actual_doc_number: str | None
    actual_action: str
    actual_priority: str
    actual_template_id: str | None
    actual_render_mode: str | None
    failures: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GoldenEvaluationSummary:
    total_cases: int
    passed_cases: int
    failed_cases: int
    doc_kind_correct: int
    doc_kind_total: int
    amount_exact_correct: int
    amount_tolerant_correct: int
    amount_total: int
    due_date_correct: int
    due_date_total: int
    action_correct: int
    action_total: int
    critical_total: int
    critical_passed: int
    e2e_total: int
    e2e_passed: int
    e2e_failed: int
    e2e_render_mode_correct: int
    e2e_render_mode_total: int
    category_summaries: tuple[GoldenBucketSummary, ...]
    subset_summaries: tuple[GoldenBucketSummary, ...]
    failure_summaries: tuple[GoldenFailureSummary, ...]
    case_results: tuple[GoldenCaseResult, ...]


@dataclass(frozen=True, slots=True)
class OfflinePipelineArtifacts:
    sender_email: str
    subject: str
    body_text: str
    attachments: tuple[dict[str, Any], ...]
    mail_type: str
    mail_type_reasons: tuple[str, ...]
    stage_order: tuple[str, ...]
    collected_facts: dict[str, Any]
    validated_facts: dict[str, Any]
    scored_facts: dict[str, Any]
    consistent_facts: dict[str, Any]
    final_facts: dict[str, Any]
    conversation_context: str
    decision: Any
    interpretation: Any


def _clone_mapping(value: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(dict(value or {}))


def _normalize_optional_text(value: str | None) -> str | None:
    token = " ".join(str(value or "").split()).strip()
    return token or None


def _parse_amount(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    token = str(value).replace("\u00a0", " ").strip()
    if not token:
        return None
    match = re.search(r"\d[\d\s]*(?:[.,]\d{1,2})?", token)
    if not match:
        return None
    normalized = match.group(0).replace(" ", "")
    if normalized.count(",") == 1 and normalized.count(".") == 0:
        frac = normalized.split(",", 1)[1]
        normalized = (
            normalized.replace(",", ".")
            if len(frac) <= 2
            else normalized.replace(",", "")
        )
    elif normalized.count(".") == 1 and normalized.count(",") == 0:
        frac = normalized.split(".", 1)[1]
        if len(frac) > 2:
            normalized = normalized.replace(".", "")
    else:
        normalized = normalized.replace(",", "").replace(".", "")
    try:
        return float(normalized)
    except ValueError:
        return None


def _amount_exact_match(expected: float | None, actual: float | None) -> bool:
    if expected is None or actual is None:
        return expected is None and actual is None
    return abs(expected - actual) <= 1e-9


def _amount_tolerant_match(expected: float | None, actual: float | None) -> bool:
    if expected is None or actual is None:
        return expected is None and actual is None
    tolerance = max(1.0, abs(expected) * 0.01)
    return abs(expected - actual) <= tolerance


def _classify_mail_type(
    *,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]],
    mail_type: str,
) -> tuple[str, tuple[str, ...]]:
    if str(mail_type or "").strip():
        return str(mail_type).strip(), ()
    mail_type_attachments = [
        pipeline_processor.MailTypeAttachment(
            filename=str(item.get("filename") or "").strip() or None,
            content_type=str(
                item.get("content_type") or item.get("type") or ""
            ).strip(),
        )
        for item in attachments
    ]
    try:
        resolved_mail_type, reasons = pipeline_processor.MailTypeClassifier.classify_detailed(
            subject=subject,
            body=body_text or "",
            attachments=mail_type_attachments,
            enable_hierarchy=getattr(
                pipeline_processor.feature_flags,
                "ENABLE_HIERARCHICAL_MAIL_TYPES",
                False,
            ),
        )
    except Exception:
        resolved_mail_type = pipeline_processor.MailTypeClassifier.classify(
            subject=subject,
            body=body_text or "",
            attachments=mail_type_attachments,
        )
        reasons = ["mt.base=fallback"]
    return str(resolved_mail_type or "").strip(), tuple(str(item) for item in reasons)


def build_offline_artifacts(
    *,
    sender_email: str,
    subject: str,
    body_text: str,
    attachments: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    mail_type: str = "",
    email_id: int = 1,
    document_id: str | None = None,
    priority: str = "\U0001F7E1",
) -> OfflinePipelineArtifacts:
    normalized_attachments = tuple(
        {
            "filename": str(item.get("filename") or ""),
            "text": str(item.get("text") or ""),
            "content_type": str(item.get("content_type") or item.get("type") or ""),
            "size_bytes": int(item.get("size_bytes") or 0),
            "metadata": dict(item.get("metadata") or {}),
        }
        for item in attachments
    )
    resolved_mail_type, mail_type_reasons = _classify_mail_type(
        subject=subject,
        body_text=body_text,
        attachments=list(normalized_attachments),
        mail_type=mail_type,
    )
    fact_body_text = pipeline_processor._main_body_for_facts(body_text)
    fact_text = " ".join(
        part
        for part in (
            subject,
            fact_body_text,
            " ".join(str(item.get("text") or "") for item in normalized_attachments),
        )
        if part
    )
    collected_facts = pipeline_processor._collect_message_facts(
        subject=subject,
        body_text=body_text,
        attachments=list(normalized_attachments),
        mail_type=resolved_mail_type,
    )
    validated_facts = pipeline_processor._validate_message_facts(
        _clone_mapping(collected_facts),
        evidence_text=fact_text,
    )
    attachment_text = " ".join(
        str(item.get("text") or "") for item in normalized_attachments
    )
    scored_facts = pipeline_processor._score_message_facts(
        _clone_mapping(validated_facts),
        evidence_text=fact_text,
        attachment_text=attachment_text,
    )
    consistent_facts = pipeline_processor._consistency_check_message_facts(
        _clone_mapping(scored_facts),
        evidence_text=fact_text,
        attachment_text=attachment_text,
    )
    normalized_evidence = pipeline_processor._normalized_lower(
        " ".join(
            part
            for part in (
                fact_text,
                " ".join(
                    str(value)
                    for value in (
                        consistent_facts.get("doc_kind"),
                        consistent_facts.get("amount"),
                        consistent_facts.get("due_date"),
                        consistent_facts.get("doc_number"),
                    )
                    if value
                ),
            )
            if part
        )
    )
    action_seed = pipeline_processor._build_heuristic_action_line(
        priority=priority,
        message_facts=consistent_facts,
    )
    short_action = pipeline_processor._select_premium_short_action(
        normalized_mail_type=pipeline_processor._normalized_lower(
            resolved_mail_type
        ).replace("_", ""),
        normalized_subject=pipeline_processor._normalized_lower(subject),
        normalized_body=pipeline_processor._normalized_lower(body_text),
        normalized_action=pipeline_processor._normalized_lower(action_seed),
        normalized_evidence=normalized_evidence,
    )
    conversation_context = pipeline_processor._detect_conversation_context(
        subject=subject,
        body_text=body_text,
        message_facts=consistent_facts,
    )
    decision = pipeline_processor._build_message_decision(
        priority=priority,
        action_line=short_action,
        summary="",
        message_facts=consistent_facts,
        sender_email=sender_email,
        subject=subject,
        body_text=body_text,
        attachments=list(normalized_attachments),
        context=conversation_context,
    )
    interpretation = pipeline_processor._build_message_interpretation(
        email_id=email_id,
        sender_email=sender_email,
        message_facts=decision.facts,
        decision=decision,
        document_id=document_id or f"offline-{email_id}",
        action=decision.action,
        priority=decision.priority,
    )
    return OfflinePipelineArtifacts(
        sender_email=sender_email,
        subject=subject,
        body_text=body_text,
        attachments=normalized_attachments,
        mail_type=resolved_mail_type,
        mail_type_reasons=mail_type_reasons,
        stage_order=(
            "collect_message_facts",
            "validate_message_facts",
            "score_message_facts",
            "consistency_check_message_facts",
            "detect_conversation_context",
            "build_message_decision",
            "build_message_interpretation",
        ),
        collected_facts=_clone_mapping(collected_facts),
        validated_facts=_clone_mapping(validated_facts),
        scored_facts=_clone_mapping(scored_facts),
        consistent_facts=_clone_mapping(consistent_facts),
        final_facts=_clone_mapping(decision.facts),
        conversation_context=str(conversation_context or ""),
        decision=decision,
        interpretation=interpretation,
    )


def _build_case_artifacts(case: GoldenCase) -> tuple[dict[str, Any], Any]:
    artifacts = build_offline_artifacts(
        sender_email=case.sender_email,
        subject=case.subject,
        body_text=case.body_text,
        attachments=[
            {"filename": item.filename, "text": item.text} for item in case.attachments
        ],
        mail_type=case.mail_type,
        email_id=1,
        document_id=f"golden-{case.case_id}",
    )
    return artifacts.final_facts, artifacts.interpretation


def _normalize_render_text(value: str | None) -> str:
    return " ".join(str(value or "").split()).casefold()


@lru_cache(maxsize=32)
def _cached_dry_run_result(fixture_name: str) -> tuple[str, str]:
    from mailbot_v26.tools.dry_run import run_dry_run_fixture

    fixture_path = DEFAULT_EML_FIXTURE_DIR / fixture_name
    result = run_dry_run_fixture(fixture_path)
    return result.render.render_mode, result.render.text


def _evaluate_dry_run_case(
    case: GoldenCase,
) -> tuple[bool, bool, bool, bool, str | None, tuple[str, ...]]:
    if not case.dry_run_validated:
        return True, True, True, True, None, ()

    fixture_name = str(case.eml_fixture or "").strip()
    if not fixture_name:
        return False, False, False, False, None, ("dry_run",)

    try:
        actual_render_mode, render_text = _cached_dry_run_result(fixture_name)
    except Exception:
        return False, False, False, False, None, ("dry_run",)

    expected_render_mode = _normalize_optional_text(case.expected_render_mode)
    normalized_render = _normalize_render_text(render_text)
    render_mode_ok = (
        True
        if expected_render_mode is None
        else _normalize_optional_text(actual_render_mode) == expected_render_mode
    )
    missing_tokens = [
        token
        for token in case.expected_render_contains
        if _normalize_render_text(token) not in normalized_render
    ]
    forbidden_tokens = [
        token
        for token in case.expected_render_not_contains
        if _normalize_render_text(token) in normalized_render
    ]
    render_contains_ok = not missing_tokens
    render_not_contains_ok = not forbidden_tokens
    failures: list[str] = []
    if not render_mode_ok:
        failures.append("render_mode")
    if not render_contains_ok:
        failures.append("render_contains")
    if not render_not_contains_ok:
        failures.append("render_not_contains")
    dry_run_ok = render_mode_ok and render_contains_ok and render_not_contains_ok
    return (
        dry_run_ok,
        render_mode_ok,
        render_contains_ok,
        render_not_contains_ok,
        _normalize_optional_text(actual_render_mode),
        tuple(failures),
    )


def evaluate_case(case: GoldenCase) -> GoldenCaseResult:
    facts, interpretation = _build_case_artifacts(case)
    expected = case.expected
    actual_doc_kind = _normalize_optional_text(interpretation.doc_kind)
    actual_amount = interpretation.amount
    actual_due_date = _normalize_optional_text(interpretation.due_date)
    actual_doc_number = _normalize_optional_text(str(facts.get("doc_number") or ""))
    actual_action = " ".join(str(interpretation.action or "").split()).strip()
    actual_priority = " ".join(str(interpretation.priority or "").split()).strip()
    actual_template_id = _normalize_optional_text(str(facts.get("template_id") or ""))

    expected_doc_kind = _normalize_optional_text(expected.doc_kind)
    expected_due_date = _normalize_optional_text(expected.due_date)
    expected_doc_number = _normalize_optional_text(expected.doc_number)
    expected_action = " ".join(str(expected.action or "").split()).strip()
    expected_priority = _normalize_optional_text(expected.priority)
    expected_template_id = _normalize_optional_text(expected.template_id)
    expected_amount = _parse_amount(expected.amount)

    doc_kind_ok = actual_doc_kind == expected_doc_kind
    amount_exact_ok = _amount_exact_match(expected_amount, actual_amount)
    amount_tolerant_ok = _amount_tolerant_match(expected_amount, actual_amount)
    due_date_ok = actual_due_date == expected_due_date
    doc_number_ok = actual_doc_number == expected_doc_number
    action_ok = actual_action == expected_action
    priority_ok = (
        True if expected_priority is None else actual_priority == expected_priority
    )
    template_ok = (
        True if expected_template_id is None else actual_template_id == expected_template_id
    )

    failures: list[str] = []
    if not doc_kind_ok:
        failures.append("doc_kind")
    if not amount_tolerant_ok:
        failures.append("amount")
    if not due_date_ok:
        failures.append("due_date")
    if not doc_number_ok:
        failures.append("doc_number")
    if not action_ok:
        failures.append("action")
    if not priority_ok:
        failures.append("priority")
    if not template_ok:
        failures.append("template_id")
    (
        dry_run_ok,
        render_mode_ok,
        render_contains_ok,
        render_not_contains_ok,
        actual_render_mode,
        render_failures,
    ) = _evaluate_dry_run_case(case)
    failures.extend(render_failures)

    forbidden_flags_ok = True
    consistency_issues = {
        str(item) for item in (facts.get("consistency_issues") or []) if str(item).strip()
    }
    for flag in expected.must_not_flags:
        normalized_flag = str(flag or "").strip()
        if not normalized_flag:
            continue
        if normalized_flag in consistency_issues:
            forbidden_flags_ok = False
            failures.append(f"must_not:{normalized_flag}")
            continue
        if normalized_flag in facts and bool(facts.get(normalized_flag)):
            forbidden_flags_ok = False
            failures.append(f"must_not:{normalized_flag}")
    passed = (
        doc_kind_ok
        and amount_tolerant_ok
        and due_date_ok
        and doc_number_ok
        and action_ok
        and priority_ok
        and template_ok
        and forbidden_flags_ok
        and dry_run_ok
    )
    return GoldenCaseResult(
        case=case,
        doc_kind_ok=doc_kind_ok,
        amount_exact_ok=amount_exact_ok,
        amount_tolerant_ok=amount_tolerant_ok,
        due_date_ok=due_date_ok,
        doc_number_ok=doc_number_ok,
        action_ok=action_ok,
        priority_ok=priority_ok,
        template_ok=template_ok,
        forbidden_flags_ok=forbidden_flags_ok,
        dry_run_ok=dry_run_ok,
        render_mode_ok=render_mode_ok,
        render_contains_ok=render_contains_ok,
        render_not_contains_ok=render_not_contains_ok,
        passed=passed,
        actual_doc_kind=actual_doc_kind,
        actual_amount=actual_amount,
        actual_due_date=actual_due_date,
        actual_doc_number=actual_doc_number,
        actual_action=actual_action,
        actual_priority=actual_priority,
        actual_template_id=actual_template_id,
        actual_render_mode=actual_render_mode,
        failures=tuple(failures),
    )


def evaluate_golden_corpus(cases: list[GoldenCase]) -> GoldenEvaluationSummary:
    results = tuple(evaluate_case(case) for case in cases)
    total_cases = len(results)
    passed_cases = sum(1 for item in results if item.passed)
    failed_cases = total_cases - passed_cases
    critical_total = sum(1 for item in results if item.case.critical)
    critical_passed = sum(
        1 for item in results if item.case.critical and item.passed
    )
    e2e_total = sum(1 for item in results if item.case.dry_run_validated)
    e2e_passed = sum(
        1 for item in results if item.case.dry_run_validated and item.dry_run_ok
    )

    category_stats: dict[str, list[int]] = {}
    subset_stats: dict[str, list[int]] = {}
    failure_type_totals: dict[str, int] = {}
    for item in results:
        category_entry = category_stats.setdefault(item.case.category, [0, 0])
        category_entry[0] += 1
        category_entry[1] += 1 if item.passed else 0
        for subset in item.case.subsets:
            subset_entry = subset_stats.setdefault(str(subset), [0, 0])
            subset_entry[0] += 1
            subset_entry[1] += 1 if item.passed else 0
        for failure in item.failures:
            failure_type_totals[str(failure)] = failure_type_totals.get(str(failure), 0) + 1

    return GoldenEvaluationSummary(
        total_cases=total_cases,
        passed_cases=passed_cases,
        failed_cases=failed_cases,
        doc_kind_correct=sum(1 for item in results if item.doc_kind_ok),
        doc_kind_total=total_cases,
        amount_exact_correct=sum(1 for item in results if item.amount_exact_ok),
        amount_tolerant_correct=sum(1 for item in results if item.amount_tolerant_ok),
        amount_total=total_cases,
        due_date_correct=sum(1 for item in results if item.due_date_ok),
        due_date_total=total_cases,
        action_correct=sum(1 for item in results if item.action_ok),
        action_total=total_cases,
        critical_total=critical_total,
        critical_passed=critical_passed,
        e2e_total=e2e_total,
        e2e_passed=e2e_passed,
        e2e_failed=e2e_total - e2e_passed,
        e2e_render_mode_correct=sum(
            1 for item in results if item.case.dry_run_validated and item.render_mode_ok
        ),
        e2e_render_mode_total=e2e_total,
        category_summaries=tuple(
            GoldenBucketSummary(
                name=name,
                total=stats[0],
                passed=stats[1],
                failed=stats[0] - stats[1],
            )
            for name, stats in sorted(category_stats.items())
        ),
        subset_summaries=tuple(
            GoldenBucketSummary(
                name=name,
                total=stats[0],
                passed=stats[1],
                failed=stats[0] - stats[1],
            )
            for name, stats in sorted(subset_stats.items())
        ),
        failure_summaries=tuple(
            GoldenFailureSummary(name=name, count=count)
            for name, count in sorted(
                failure_type_totals.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        case_results=results,
    )


def render_summary(summary: GoldenEvaluationSummary) -> str:
    lines = [
        "Golden corpus evaluator",
        f"Total cases: {summary.total_cases}",
        f"Passed: {summary.passed_cases}",
        f"Failed: {summary.failed_cases}",
        f"Critical safety: {summary.critical_passed}/{summary.critical_total}",
        f"Doc kind accuracy: {summary.doc_kind_correct}/{summary.doc_kind_total}",
        f"Amount exact match: {summary.amount_exact_correct}/{summary.amount_total}",
        f"Amount tolerant match: {summary.amount_tolerant_correct}/{summary.amount_total}",
        f"Due date accuracy: {summary.due_date_correct}/{summary.due_date_total}",
        f"Action accuracy: {summary.action_correct}/{summary.action_total}",
        f"E2E dry-run: {summary.e2e_passed}/{summary.e2e_total}",
        (
            f"E2E render mode accuracy: "
            f"{summary.e2e_render_mode_correct}/{summary.e2e_render_mode_total}"
        ),
        "Categories:",
    ]
    if not summary.category_summaries:
        lines.append("- none")
    else:
        for item in summary.category_summaries:
            lines.append(
                f"- {item.name}: {item.passed}/{item.total} passed ({item.failed} failed)"
            )
    lines.append("Subsets:")
    if not summary.subset_summaries:
        lines.append("- none")
    else:
        for item in summary.subset_summaries:
            lines.append(
                f"- {item.name}: {item.passed}/{item.total} passed ({item.failed} failed)"
            )
    lines.extend(
        [
            "Failure types:",
        ]
    )
    if not summary.failure_summaries:
        lines.append("- none")
    else:
        for item in summary.failure_summaries:
            lines.append(f"- {item.name}: {item.count}")
    lines.extend(
        [
        "Failures by case_id:",
        ]
    )
    failed = [item for item in summary.case_results if not item.passed]
    if not failed:
        lines.append("- none")
    else:
        for item in failed:
            lines.append(f"- {item.case.case_id}: {', '.join(item.failures)}")
    return "\n".join(lines)


def render_report(
    summary: GoldenEvaluationSummary,
    *,
    generated_at: str,
) -> str:
    total = max(1, int(summary.total_cases))
    pass_rate = (float(summary.passed_cases) / float(total)) * 100.0
    critical_failed = [
        item for item in summary.case_results if item.case.critical and not item.passed
    ]
    weak_categories = [
        item for item in summary.category_summaries if item.failed > 0
    ]
    e2e_cases = [item for item in summary.case_results if item.case.dry_run_validated]
    dangerous_render_failures = [
        item
        for item in e2e_cases
        if (not item.dry_run_ok)
        and (
            item.case.critical
            or not item.render_not_contains_ok
            or not item.render_mode_ok
        )
    ]
    promotion_cases = [
        item for item in summary.case_results if "correction_sensitive" in item.case.subsets
    ]
    would_promote = sum(
        1
        for item in promotion_cases
        if item.passed and str(item.actual_template_id or "").strip()
    )
    blocked = max(0, len(promotion_cases) - would_promote)
    lines = [
        "=== LETTERBOT GOLDEN CORPUS REPORT ===",
        f"Date: {generated_at}",
        (
            f"Total cases: {summary.total_cases}  |  Passed: {summary.passed_cases}  |  "
            f"Failed: {summary.failed_cases}  |  Pass rate: {pass_rate:.1f}%"
        ),
        "",
        f"CRITICAL CASES: {summary.critical_passed}/{summary.critical_total} passed",
        "DANGEROUS FAILURES:",
    ]
    if not critical_failed:
        lines.append("none")
    else:
        for item in critical_failed:
            lines.append(f"- {item.case.case_id}: {', '.join(item.failures)}")
    lines.extend(["", "CATEGORY BREAKDOWN:"])
    for bucket in summary.category_summaries:
        pct = (float(bucket.passed) / float(max(1, bucket.total))) * 100.0
        lines.append(f"{bucket.name:<32}: {bucket.passed}/{bucket.total}  ({pct:.0f}%)")
    lines.extend(
        [
            "",
            "E2E DRY-RUN CASES:",
            (
                f"Total: {summary.e2e_total}  |  Passed: {summary.e2e_passed}  |  "
                f"Failed: {summary.e2e_failed}"
            ),
            (
                "Render mode accuracy: "
                f"{(float(summary.e2e_render_mode_correct) / float(max(1, summary.e2e_render_mode_total))) * 100.0:.1f}%"
            ),
            "Dangerous render failures:",
        ]
    )
    if not dangerous_render_failures:
        lines.append("none")
    else:
        for item in dangerous_render_failures:
            lines.append(f"- {item.case.case_id}: {', '.join(item.failures)}")
    lines.extend(["", "WEAK CATEGORIES (< 100%):"])
    if not weak_categories:
        lines.append("none")
    else:
        for bucket in weak_categories:
            lines.append(f"{bucket.name}: {bucket.failed} failed")
            for item in summary.case_results:
                if item.case.category != bucket.name or item.passed:
                    continue
                lines.append(
                    f"  - {item.case.case_id}: expected {item.case.expected.action}, got {item.actual_action}"
                )
    lines.extend(
        [
            "",
            "PROMOTION SHADOW CASES:",
            f"Candidates: {len(promotion_cases)}  |  Would-promote: {would_promote}  |  Blocked: {blocked}",
        ]
    )
    return "\n".join(lines)


@lru_cache(maxsize=4)
def load_golden_corpus(path_str: str | None = None) -> tuple[GoldenCase, ...]:
    path = Path(path_str) if path_str else DEFAULT_CORPUS_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases: list[GoldenCase] = []
    for raw_case in payload:
        expected_payload = raw_case.get("expected") or {}
        attachments = tuple(
            GoldenAttachment(
                filename=str(item.get("filename") or ""),
                text=str(item.get("text") or ""),
            )
            for item in (raw_case.get("attachments") or [])
        )
        expected = GoldenExpected(
            doc_kind=_normalize_optional_text(expected_payload.get("doc_kind")),
            amount=expected_payload.get("amount"),
            due_date=_normalize_optional_text(expected_payload.get("due_date")),
            doc_number=_normalize_optional_text(expected_payload.get("doc_number")),
            action=str(expected_payload.get("action") or "").strip(),
            priority=_normalize_optional_text(expected_payload.get("priority")),
            template_id=_normalize_optional_text(expected_payload.get("template_id")),
            must_not_flags=tuple(
                str(item)
                for item in (expected_payload.get("must_not_flags") or [])
                if str(item).strip()
            ),
        )
        cases.append(
            GoldenCase(
                case_id=str(raw_case.get("case_id") or ""),
                category=str(raw_case.get("category") or "uncategorized"),
                sender_email=str(raw_case.get("sender_email") or ""),
                subject=str(raw_case.get("subject") or ""),
                body_text=str(raw_case.get("body_text") or ""),
                attachments=attachments,
                expected=expected,
                critical=bool(raw_case.get("critical")),
                mail_type=str(raw_case.get("mail_type") or ""),
                subsets=tuple(
                    str(item)
                    for item in (raw_case.get("subsets") or [])
                    if str(item).strip()
                ),
                dry_run_validated=bool(raw_case.get("dry_run_validated")),
                eml_fixture=_normalize_optional_text(raw_case.get("eml_fixture")),
                expected_render_mode=_normalize_optional_text(
                    raw_case.get("expected_render_mode")
                ),
                expected_render_contains=tuple(
                    str(item)
                    for item in (raw_case.get("expected_render_contains") or [])
                    if str(item).strip()
                ),
                expected_render_not_contains=tuple(
                    str(item)
                    for item in (raw_case.get("expected_render_not_contains") or [])
                    if str(item).strip()
                ),
            )
        )
    return tuple(cases)


def _report_timestamp(path: Path) -> str:
    try:
        ts = float(path.stat().st_mtime)
    except OSError:
        ts = 0.0
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run deterministic golden-corpus evaluation for the canonical Letterbot pipeline."
    )
    parser.add_argument(
        "--path",
        default=str(DEFAULT_CORPUS_PATH),
        help="Path to the golden corpus JSON fixture.",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print a developer-facing quality report instead of the default summary.",
    )
    args = parser.parse_args(argv)
    cases = list(load_golden_corpus(args.path))
    summary = evaluate_golden_corpus(cases)
    if args.report:
        print(render_report(summary, generated_at=_report_timestamp(Path(args.path))))
    else:
        print(render_summary(summary))
    return 0 if summary.failed_cases == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_CORPUS_PATH",
    "GoldenAttachment",
    "OfflinePipelineArtifacts",
    "GoldenCase",
    "GoldenCaseResult",
    "GoldenEvaluationSummary",
    "GoldenExpected",
    "GoldenBucketSummary",
    "GoldenFailureSummary",
    "build_offline_artifacts",
    "evaluate_case",
    "evaluate_golden_corpus",
    "load_golden_corpus",
    "render_report",
    "render_summary",
]
