from __future__ import annotations

from dataclasses import dataclass

from mailbot_v26.config.trust_bootstrap import TrustBootstrapConfig
from mailbot_v26.storage.analytics import KnowledgeAnalytics

_DAY_SECONDS = 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class TrustBootstrapSnapshot:
    start_ts: float | None
    days_since_start: float | None
    samples_count: int
    corrections_count: int
    surprises_count: int
    surprise_rate: float | None
    active: bool


def compute_trust_bootstrap_snapshot(
    *,
    analytics: KnowledgeAnalytics,
    account_email: str,
    now_ts: float,
    config: TrustBootstrapConfig,
) -> TrustBootstrapSnapshot:
    start_ts = analytics.bootstrap_start_ts(account_email=account_email)
    if start_ts is None:
        return TrustBootstrapSnapshot(
            start_ts=None,
            days_since_start=None,
            samples_count=0,
            corrections_count=0,
            surprises_count=0,
            surprise_rate=None,
            active=False,
        )

    days_since_start = max(0.0, (now_ts - start_ts) / _DAY_SECONDS)
    samples_count = analytics.bootstrap_samples_count(
        account_email=account_email,
        start_ts=start_ts,
    )
    window_days = min(7, max(1, config.learning_days))
    corrections_since_ts = now_ts - (window_days * _DAY_SECONDS)
    corrections_count = analytics.bootstrap_corrections_count(
        account_email=account_email,
        since_ts=corrections_since_ts,
    )
    surprises_count = analytics.bootstrap_surprises_count(
        account_email=account_email,
        since_ts=corrections_since_ts,
    )
    surprise_rate = (
        surprises_count / corrections_count
        if corrections_count > 0
        else None
    )

    active = False
    if days_since_start < config.learning_days:
        active = True
    if samples_count < config.min_samples:
        active = True
    if (
        corrections_count > 0
        and surprise_rate is not None
        and surprise_rate > config.max_allowed_surprise_rate
    ):
        active = True

    return TrustBootstrapSnapshot(
        start_ts=start_ts,
        days_since_start=days_since_start,
        samples_count=samples_count,
        corrections_count=corrections_count,
        surprises_count=surprises_count,
        surprise_rate=surprise_rate,
        active=active,
    )


def is_bootstrap_active(
    account_email: str,
    now_ts: float,
    *,
    analytics: KnowledgeAnalytics,
    config: TrustBootstrapConfig,
) -> bool:
    snapshot = compute_trust_bootstrap_snapshot(
        analytics=analytics,
        account_email=account_email,
        now_ts=now_ts,
        config=config,
    )
    return snapshot.active


__all__ = ["TrustBootstrapSnapshot", "compute_trust_bootstrap_snapshot", "is_bootstrap_active"]
