"""WP6: no discarded attention computation; no zero-corrections quality line."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mailbot_v26.insights.quality_metrics import CountBreakdown, QualityMetricsSnapshot
from mailbot_v26.pipeline import daily_digest, weekly_digest
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB

NOW = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)


def _count_attention_calls(monkeypatch, module) -> list[int]:
    windows: list[int] = []

    def _fake(**kwargs):
        windows.append(int(kwargs["window_days"]))
        return None

    monkeypatch.setattr(module, "compute_attention_economics", _fake)
    return windows


def _analytics(tmp_path) -> KnowledgeAnalytics:
    db_path = tmp_path / "digest.sqlite"
    KnowledgeDB(db_path)
    return KnowledgeAnalytics(db_path)


def test_daily_digest_computes_attention_once_for_its_own_window(
    monkeypatch, tmp_path
) -> None:
    windows = _count_attention_calls(monkeypatch, daily_digest)
    # No accounts.ini lookup outside tmp_path.
    monkeypatch.setattr(daily_digest, "resolve_account_scope", lambda *_a, **_k: None)

    daily_digest._collect_digest_data(
        analytics=_analytics(tmp_path),
        account_email="account@example.com",
        include_attention_economics=True,
        now=NOW,
    )

    assert windows == [7]


def test_weekly_digest_computes_attention_once_for_its_own_window(
    monkeypatch, tmp_path
) -> None:
    windows = _count_attention_calls(monkeypatch, weekly_digest)

    weekly_digest._collect_weekly_data(
        analytics=_analytics(tmp_path),
        account_email="account@example.com",
        account_emails=["account@example.com"],
        week_key="2026-W43",
        include_attention_economics=True,
        now=NOW,
    )

    assert windows == [7]


def _quality(corrections: int) -> QualityMetricsSnapshot:
    return QualityMetricsSnapshot(
        window_days=1,
        corrections_total=corrections,
        by_new_priority=(
            [CountBreakdown(key="high", count=corrections)] if corrections else []
        ),
        by_engine=[],
        correction_rate=(corrections / 10) if corrections else 0.0,
        emails_received=10,
    )


def _digest(quality: QualityMetricsSnapshot) -> daily_digest.DigestData:
    return daily_digest.DigestData(
        deferred_total=1,
        deferred_attachments_only=1,
        deferred_informational=0,
        deferred_items=[],
        uncertainty_queue_items=[],
        commitments_pending=0,
        commitments_expired=0,
        trust_delta=None,
        health_delta=None,
        anomaly_alerts=[],
        attention_economics=None,
        quality_metrics=quality,
        notification_sla=None,
        deadlock_insights=[],
        silence_insights=[],
        digest_insights_enabled=False,
        digest_insights_max_items=0,
        digest_action_templates_enabled=False,
    )


@pytest.mark.parametrize(
    ("locale", "label"), [("en", "Quality:"), ("ru", "Качество:")]
)
def test_daily_quality_line_needs_at_least_one_correction(
    locale: str, label: str
) -> None:
    quiet = daily_digest._build_digest_text(_digest(_quality(0)), locale=locale)
    busy = daily_digest._build_digest_text(_digest(_quality(2)), locale=locale)

    assert label not in quiet
    assert label in busy
