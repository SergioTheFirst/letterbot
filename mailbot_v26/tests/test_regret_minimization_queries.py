from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.events.contract import EventType, EventV1
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def _emit_event(
    emitter: ContractEventEmitter,
    *,
    event_type: EventType,
    ts_utc: float,
    account_id: str,
    entity_id: str | None,
    payload: dict[str, object],
) -> None:
    emitter.emit(
        EventV1(
            event_type=event_type,
            ts_utc=ts_utc,
            account_id=account_id,
            entity_id=entity_id,
            email_id=None,
            payload=payload,
        )
    )


def test_regret_minimization_returns_none_below_min_samples(tmp_path: Path) -> None:
    db_path = tmp_path / "regret.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=(now - timedelta(days=5)).timestamp(),
        account_id="acc@example.com",
        entity_id="entity-1",
        payload={"commitment_id": 1},
    )

    result = analytics.regret_minimization_stats(
        account_email="acc@example.com",
        window_days=30,
        trust_drop_window_days=7,
        min_samples=2,
        now_dt=now,
    )

    assert result is None


def test_regret_minimization_counts_trust_drops(tmp_path: Path) -> None:
    db_path = tmp_path / "regret.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)
    account_id = "acc@example.com"

    expired_a = now - timedelta(days=10)
    expired_b = now - timedelta(days=20)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_a.timestamp(),
        account_id=account_id,
        entity_id="entity-a",
        payload={"commitment_id": 1},
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_b.timestamp(),
        account_id=account_id,
        entity_id="entity-b",
        payload={"commitment_id": 2},
    )

    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=1)).timestamp(),
        account_id=account_id,
        entity_id="entity-a",
        payload={"trust_score": 0.9},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=3)).timestamp(),
        account_id=account_id,
        entity_id="entity-a",
        payload={"trust_score": 0.7},
    )
    _emit_event(
        emitter,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        ts_utc=(expired_b + timedelta(days=1)).timestamp(),
        account_id=account_id,
        entity_id="entity-b",
        payload={"health_score": 80.0},
    )
    _emit_event(
        emitter,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        ts_utc=(expired_b + timedelta(days=2)).timestamp(),
        account_id=account_id,
        entity_id="entity-b",
        payload={"health_score": 85.0},
    )

    result = analytics.regret_minimization_stats(
        account_email=account_id,
        window_days=90,
        trust_drop_window_days=7,
        min_samples=2,
        now_dt=now,
    )

    assert result == {"total": 2, "drops": 1, "pct": 50}


def test_regret_minimization_scopes_account_emails(tmp_path: Path) -> None:
    db_path = tmp_path / "regret_scope.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)

    expired_a = now - timedelta(days=10)
    expired_b = now - timedelta(days=12)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_a.timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"commitment_id": 1},
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_b.timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={"commitment_id": 2},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=1)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"trust_score": 0.9},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=2)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"trust_score": 0.6},
    )
    _emit_event(
        emitter,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        ts_utc=(expired_b + timedelta(days=1)).timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={"health_score": 90.0},
    )
    _emit_event(
        emitter,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        ts_utc=(expired_b + timedelta(days=2)).timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={"health_score": 80.0},
    )

    result = analytics.regret_minimization_stats(
        account_email="acc-a@example.com",
        account_emails=["acc-a@example.com", "acc-b@example.com"],
        window_days=90,
        trust_drop_window_days=7,
        min_samples=2,
        now_dt=now,
    )

    assert result == {"total": 2, "drops": 2, "pct": 100}


def test_regret_minimization_empty_scope_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "regret_empty.sqlite"
    analytics = KnowledgeAnalytics(db_path)
    emitter = ContractEventEmitter(db_path)
    now = datetime(2024, 7, 10, tzinfo=timezone.utc)

    expired_a = now - timedelta(days=10)
    expired_b = now - timedelta(days=12)

    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_a.timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"commitment_id": 1},
    )
    _emit_event(
        emitter,
        event_type=EventType.COMMITMENT_EXPIRED,
        ts_utc=expired_b.timestamp(),
        account_id="acc-b@example.com",
        entity_id="entity-b",
        payload={"commitment_id": 2},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=1)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"trust_score": 0.9},
    )
    _emit_event(
        emitter,
        event_type=EventType.TRUST_SCORE_UPDATED,
        ts_utc=(expired_a + timedelta(days=2)).timestamp(),
        account_id="acc-a@example.com",
        entity_id="entity-a",
        payload={"trust_score": 0.6},
    )

    result = analytics.regret_minimization_stats(
        account_email="acc-a@example.com",
        account_emails=[],
        window_days=90,
        trust_drop_window_days=7,
        min_samples=1,
        now_dt=now,
    )

    assert result == {"total": 1, "drops": 1, "pct": 100}
