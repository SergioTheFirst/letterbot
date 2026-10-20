"""WP6: no discarded attention computation; no zero-corrections quality line."""

from __future__ import annotations

from datetime import datetime, timezone

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
