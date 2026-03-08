from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mailbot_v26.insights.anomaly_engine import compute_anomalies
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.context_layer import ContextStore
from mailbot_v26.storage.knowledge_db import KnowledgeDB


def _seed_entity(db_path, from_email: str) -> str:
    store = ContextStore(db_path)
    resolution = store.resolve_sender_entity(
        from_email=from_email,
        from_name="Sender",
    )
    assert resolution is not None
    return resolution.entity_id


def _seed_response_times(
    db_path, entity_id: str, entries: list[tuple[datetime, float]]
) -> None:
    store = ContextStore(db_path)
    for event_time, hours in entries:
        store.record_interaction_event(
            entity_id=entity_id,
            event_type="response_time",
            event_time=event_time,
            metadata={"response_time_hours": hours},
        )


def _seed_email_events(db_path, entity_id: str, events: list[datetime]) -> None:
    store = ContextStore(db_path)
    for event_time in events:
        store.record_interaction_event(
            entity_id=entity_id,
            event_type="email_received",
            event_time=event_time,
            metadata={"from_email": "sender@example.com"},
        )


def test_anomaly_rt_alert(tmp_path) -> None:
    now = datetime(2024, 5, 20, 12, 0, tzinfo=timezone.utc)
    db_path = tmp_path / "anomaly_rt.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        [
            (now - timedelta(days=10), 2.0),
            (now - timedelta(days=9), 2.0),
            (now - timedelta(days=8), 2.0),
            (now - timedelta(hours=1), 6.0),
        ],
    )

    anomalies = compute_anomalies(
        entity_id=entity_id,
        analytics=KnowledgeAnalytics(db_path),
        now_dt=now,
    )

    assert any(
        anomaly.type == "RESPONSE_TIME_DELAY" and anomaly.severity == "ALERT"
        for anomaly in anomalies
    )


def test_anomaly_insufficient_data(tmp_path) -> None:
    now = datetime(2024, 5, 20, 12, 0, tzinfo=timezone.utc)
    db_path = tmp_path / "anomaly_insufficient.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    _seed_response_times(
        db_path,
        entity_id,
        [
            (now - timedelta(days=3), 4.0),
            (now - timedelta(hours=2), 8.0),
        ],
    )

    anomalies = compute_anomalies(
        entity_id=entity_id,
        analytics=KnowledgeAnalytics(db_path),
        now_dt=now,
    )

    assert anomalies == []


def test_anomaly_frequency_logic(tmp_path) -> None:
    now = datetime(2024, 5, 20, 12, 0, tzinfo=timezone.utc)
    db_path = tmp_path / "anomaly_frequency.sqlite"
    KnowledgeDB(db_path)
    entity_id = _seed_entity(db_path, "sender@example.com")
    events = [now - timedelta(days=day) for day in range(8, 28)]
    _seed_email_events(db_path, entity_id, events)

    anomalies = compute_anomalies(
        entity_id=entity_id,
        analytics=KnowledgeAnalytics(db_path),
        now_dt=now,
    )

    assert any(
        anomaly.type == "FREQUENCY_DROP" and anomaly.severity == "WARN"
        for anomaly in anomalies
    )
