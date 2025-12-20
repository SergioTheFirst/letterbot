from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

from mailbot_v26.storage.context_layer import ContextStore, normalize_name


def test_entity_created_for_new_sender(tmp_path) -> None:
    db_path = tmp_path / "context.sqlite"
    store = ContextStore(db_path)

    resolution = store.resolve_sender_entity(
        from_email="ivan@example.com",
        from_name="Ivanov Ivan",
    )

    assert resolution is not None
    assert resolution.confidence == 1.0

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT name, normalized_name FROM entities WHERE id = ?;",
            (resolution.entity_id,),
        ).fetchone()

    assert row is not None
    assert row[0] == "Ivanov Ivan"
    assert row[1] == normalize_name("Ivanov Ivan")


def test_entity_resolution_matches_normalized_name(tmp_path) -> None:
    db_path = tmp_path / "context.sqlite"
    store = ContextStore(db_path)

    first = store.resolve_sender_entity(
        from_email="ivan@example.com",
        from_name="Ivanov Ivan",
    )
    second = store.resolve_sender_entity(
        from_email="ivan@example.com",
        from_name="ivanov ivan",
    )

    assert first is not None
    assert second is not None
    assert first.entity_id == second.entity_id

    with sqlite3.connect(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM entities;").fetchone()[0]

    assert total == 1


def test_recompute_email_frequency_baseline(tmp_path) -> None:
    db_path = tmp_path / "context.sqlite"
    store = ContextStore(db_path)
    resolution = store.resolve_sender_entity(
        from_email="sender@example.com",
        from_name="Sender",
    )
    assert resolution is not None

    now = datetime.utcnow()
    for i in range(15):
        store.record_interaction_event(
            entity_id=resolution.entity_id,
            event_type="email_received",
            event_time=now - timedelta(days=i),
            metadata={"email_id": i},
        )

    baseline_value, sample_size = store.recompute_email_frequency(
        entity_id=resolution.entity_id,
        now=now,
    )

    assert sample_size == 15
    assert baseline_value == pytest.approx(0.5)
