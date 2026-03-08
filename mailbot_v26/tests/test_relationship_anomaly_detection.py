from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from mailbot_v26.insights.relationship_anomaly import RelationshipAnomalyDetector
from mailbot_v26.insights.trust_score import (
    TrustScoreComponents,
    TrustScoreResult,
    TrustSnapshot,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


class StubTrustScoreCalculator:
    def __init__(self, scores: dict[int, float | None]) -> None:
        self.scores = scores

    def compute(
        self,
        *,
        entity_id: str,
        from_email: str | None,
        response_window_days: int | None = None,
        trend_window_days: int | None = None,
    ) -> TrustScoreResult:
        score = self.scores.get(response_window_days or 0)
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


def _insert_email(
    conn: sqlite3.Connection, *, from_email: str, received_at: str
) -> int:
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


def _insert_commitment(
    conn: sqlite3.Connection, *, email_row_id: int, status: str
) -> None:
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


def _seed_response_times(
    db_path, entity_id: str, now: datetime, entries: list[tuple[int, float]]
) -> None:
    store = ContextStore(db_path)
    for offset, hours in entries:
        store.record_interaction_event(
            entity_id=entity_id,
            event_type="response_time",
            event_time=now - timedelta(days=offset),
            metadata={"response_time_hours": hours},
        )


def test_response_time_spike_detected(tmp_path) -> None:
    now = datetime.utcnow()
    db_path = tmp_path / "response_spike.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        now,
        [
            (5, 10.0),
            (10, 10.0),
            (20, 10.0),
            (40, 1.0),
            (70, 1.0),
            (80, 1.0),
            (85, 1.0),
            (88, 1.0),
            (89, 1.0),
        ],
    )
    detector = RelationshipAnomalyDetector(
        KnowledgeAnalytics(db_path),
        StubTrustScoreCalculator({30: 0.8, 60: 0.8, 120: 0.8}),
    )

    anomalies = detector.detect(entity_id=entity_id, from_email="sender@example.com")

    assert any(anomaly.anomaly_type == "RESPONSE_TIME_SPIKE" for anomaly in anomalies)


def test_commitment_break_pattern_detected(tmp_path) -> None:
    now = datetime.utcnow()
    db_path = tmp_path / "commitment_break.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        now,
        [(5, 2.0), (20, 2.0), (40, 2.0), (70, 2.0), (110, 2.0)],
    )
    with sqlite3.connect(db_path) as conn:
        for _ in range(2):
            email_row_id = _insert_email(
                conn,
                from_email="sender@example.com",
                received_at=now.isoformat(),
            )
            _insert_commitment(conn, email_row_id=email_row_id, status="expired")

    detector = RelationshipAnomalyDetector(
        KnowledgeAnalytics(db_path),
        StubTrustScoreCalculator({30: 0.8, 60: 0.8, 120: 0.8}),
    )

    anomalies = detector.detect(entity_id=entity_id, from_email="sender@example.com")

    assert any(
        anomaly.anomaly_type == "COMMITMENT_BREAK_PATTERN" for anomaly in anomalies
    )


def test_relationship_health_drop_detected(tmp_path) -> None:
    now = datetime.utcnow()
    db_path = tmp_path / "health_drop.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        now,
        [(5, 2.0), (20, 2.0), (40, 2.0), (70, 2.0), (110, 2.0)],
    )
    with sqlite3.connect(db_path) as conn:
        email_row_id = _insert_email(
            conn,
            from_email="sender@example.com",
            received_at=now.isoformat(),
        )
        _insert_commitment(conn, email_row_id=email_row_id, status="fulfilled")

    detector = RelationshipAnomalyDetector(
        KnowledgeAnalytics(db_path),
        StubTrustScoreCalculator({30: 0.4, 60: 0.8, 120: 0.8}),
    )

    anomalies = detector.detect(entity_id=entity_id, from_email="sender@example.com")

    assert any(
        anomaly.anomaly_type == "RELATIONSHIP_HEALTH_DROP" for anomaly in anomalies
    )


def test_insufficient_data_yields_no_anomaly(tmp_path) -> None:
    db_path = tmp_path / "insufficient.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")

    detector = RelationshipAnomalyDetector(
        KnowledgeAnalytics(db_path),
        StubTrustScoreCalculator({30: None, 60: None, 120: None}),
    )

    anomalies = detector.detect(entity_id=entity_id, from_email="sender@example.com")

    assert anomalies == []
