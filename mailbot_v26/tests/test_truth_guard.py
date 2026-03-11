from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mailbot_v26.system.truth_guard import (
    TRUTH_HIERARCHY,
    assert_not_projection_source,
    mark_as_stale,
    truth_rank,
)


def test_truth_hierarchy_respected() -> None:
    assert TRUTH_HIERARCHY == (
        "events_v1",
        "persisted_runtime_state",
        "projections",
        "ui_summaries",
    )
    assert truth_rank("events_v1") < truth_rank("persisted_runtime_state")
    assert truth_rank("persisted_runtime_state") < truth_rank("projections")
    assert truth_rank("projections") < truth_rank("ui_summaries")


def test_projection_not_used_as_truth(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        allowed = assert_not_projection_source(
            source="projection",
            context="web_health_component:telegram",
        )

    assert allowed is False
    assert any(record.message == "projection_used_as_truth" for record in caplog.records)


def test_mark_as_stale_reports_old_projection_snapshot(caplog) -> None:
    now = datetime.now(timezone.utc)
    with caplog.at_level(logging.WARNING):
        state = mark_as_stale(
            source="processing_spans",
            snapshot_ts=(now - timedelta(hours=2)).isoformat(),
            now_ts=now.timestamp(),
            threshold_seconds=900,
            context="web_health_status",
        )

    assert state["stale"] is True
    assert float(state["age_seconds"] or 0.0) >= 7_200 - 1
    assert any(record.message == "snapshot_marked_stale" for record in caplog.records)
