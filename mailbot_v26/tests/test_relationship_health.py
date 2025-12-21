from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from mailbot_v26.insights.relationship_health import RelationshipHealthCalculator
from mailbot_v26.insights.trust_score import (
    TrustScoreComponents,
    TrustScoreResult,
    TrustSnapshot,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


class StubTrustScoreCalculator:
    def __init__(self, scores: dict[tuple[int | None, int | None] | None, float | None]) -> None:
        self.scores = scores

    def compute(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        response_window_days: int | None = None,
        trend_window_days: int | None = None,
    ) -> TrustScoreResult:
        score = self.scores.get(
            (response_window_days, trend_window_days),
            self.scores.get(None),
        )
        return TrustScoreResult(
            snapshot=TrustSnapshot(
                entity_id=entity_id,
                score=score,
                reason=None if score is not None else "insufficient_data",
                sample_size=3,
            ),
            components=TrustScoreComponents(
                commitment_reliability=None,
                response_consistency=None,
                trend=None,
            ),
            data_window_days=response_window_days or 60,
        )


def _seed_entity(db_path, from_email: str) -> str:
    store = ContextStore(db_path)
    resolution = store.resolve_sender_entity(
        from_email=from_email,
        from_name="Sender",
    )
    assert resolution is not None
    return resolution.entity_id


def _insert_email(conn: sqlite3.Connection, *, from_email: str, received_at: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO emails (
            account_email,
            from_email,
            subject,
            received_at,
            priority
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("account@example.com", from_email, "Subject", received_at, "🔵"),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_commitment(conn: sqlite3.Connection, *, email_row_id: int, status: str) -> None:
    conn.execute(
        """
        INSERT INTO commitments (
            email_row_id,
            source,
            commitment_text,
            deadline_iso,
            status,
            confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            email_row_id,
            "email",
            "Send report",
            None,
            status,
            1.0,
        ),
    )
    conn.commit()


def _seed_response_times(db_path, entity_id: str, now: datetime, entries: list[tuple[int, float]]) -> None:
    store = ContextStore(db_path)
    for offset, hours in entries:
        store.record_interaction_event(
            entity_id=entity_id,
            event_type="response_time",
            event_time=now - timedelta(days=offset),
            metadata={"response_time_hours": hours},
        )


def _compute_health_score(
    *,
    db_path,
    from_email: str,
    entity_id: str,
    trust_scores: dict[tuple[int | None, int | None] | None, float | None],
) -> float | None:
    analytics = KnowledgeAnalytics(db_path)
    calculator = RelationshipHealthCalculator(analytics, StubTrustScoreCalculator(trust_scores))
    snapshot = calculator.compute(entity_id=entity_id, from_email=from_email)
    return snapshot.health_score


def test_relationship_health_insufficient_history(tmp_path) -> None:
    db_path = tmp_path / "rhs.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")

    analytics = KnowledgeAnalytics(db_path)
    calculator = RelationshipHealthCalculator(
        analytics,
        StubTrustScoreCalculator({None: None}),
    )
    snapshot = calculator.compute(entity_id=entity_id, from_email="sender@example.com")

    assert snapshot.health_score is None
    assert snapshot.reason == "insufficient_history"


def test_relationship_health_response_time_anomaly_decreases_score(tmp_path) -> None:
    now = datetime.utcnow()

    def _build_db(name: str, response_entries: list[tuple[int, float]]) -> float | None:
        db_path = tmp_path / f"{name}.sqlite"
        KnowledgeDB(db_path)
        entity_id = _seed_entity(db_path, "sender@example.com")
        _seed_response_times(db_path, entity_id, now, response_entries)
        with sqlite3.connect(db_path) as conn:
            for _ in range(2):
                email_row_id = _insert_email(
                    conn,
                    from_email="sender@example.com",
                    received_at=now.isoformat(),
                )
                _insert_commitment(conn, email_row_id=email_row_id, status="fulfilled")
        return _compute_health_score(
            db_path=db_path,
            from_email="sender@example.com",
            entity_id=entity_id,
            trust_scores={
                None: 0.8,
                (30, 30): 0.8,
                (60, 60): 0.8,
            },
        )

    baseline_score = _build_db(
        "rhs_baseline",
        [(5, 2.0), (10, 2.0), (20, 2.0), (40, 2.0), (70, 2.0), (80, 2.0)],
    )
    degraded_score = _build_db(
        "rhs_degraded",
        [(5, 10.0), (10, 10.0), (20, 10.0), (40, 1.0), (70, 1.0), (80, 1.0)],
    )

    assert baseline_score is not None
    assert degraded_score is not None
    assert degraded_score < baseline_score


def test_relationship_health_commitment_expired_influences_score(tmp_path) -> None:
    now = datetime.utcnow()

    def _build_db(name: str, statuses: list[str]) -> float | None:
        db_path = tmp_path / f"{name}.sqlite"
        KnowledgeDB(db_path)
        entity_id = _seed_entity(db_path, "sender@example.com")
        _seed_response_times(
            db_path,
            entity_id,
            now,
            [(5, 2.0), (10, 2.0), (20, 2.0), (40, 2.0), (70, 2.0)],
        )
        with sqlite3.connect(db_path) as conn:
            for status in statuses:
                email_row_id = _insert_email(
                    conn,
                    from_email="sender@example.com",
                    received_at=now.isoformat(),
                )
                _insert_commitment(conn, email_row_id=email_row_id, status=status)
        return _compute_health_score(
            db_path=db_path,
            from_email="sender@example.com",
            entity_id=entity_id,
            trust_scores={
                None: 0.8,
                (30, 30): 0.8,
                (60, 60): 0.8,
            },
        )

    healthy_score = _build_db("rhs_commitment_ok", ["fulfilled", "fulfilled"])
    expired_score = _build_db("rhs_commitment_bad", ["expired", "expired"])

    assert healthy_score is not None
    assert expired_score is not None
    assert expired_score < healthy_score


def test_relationship_health_negative_trend_decreases_score(tmp_path) -> None:
    now = datetime.utcnow()
    db_path = tmp_path / "rhs_trend.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        now,
        [(5, 2.0), (10, 2.0), (20, 2.0), (40, 2.0), (70, 2.0)],
    )
    with sqlite3.connect(db_path) as conn:
        for _ in range(2):
            email_row_id = _insert_email(
                conn,
                from_email="sender@example.com",
                received_at=now.isoformat(),
            )
            _insert_commitment(conn, email_row_id=email_row_id, status="fulfilled")

    analytics = KnowledgeAnalytics(db_path)

    positive_calculator = RelationshipHealthCalculator(
        analytics,
        StubTrustScoreCalculator(
            {
                None: 0.8,
                (30, 30): 0.9,
                (60, 60): 0.7,
            }
        ),
    )
    negative_calculator = RelationshipHealthCalculator(
        analytics,
        StubTrustScoreCalculator(
            {
                None: 0.8,
                (30, 30): 0.6,
                (60, 60): 0.85,
            }
        ),
    )

    positive_snapshot = positive_calculator.compute(entity_id=entity_id, from_email="sender@example.com")
    negative_snapshot = negative_calculator.compute(entity_id=entity_id, from_email="sender@example.com")

    assert positive_snapshot.health_score is not None
    assert negative_snapshot.health_score is not None
    assert negative_snapshot.health_score < positive_snapshot.health_score
