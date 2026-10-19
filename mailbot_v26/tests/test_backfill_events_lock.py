from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from mailbot_v26.tools.backfill_events import run_backfill

# The observability emitter waits out SQLite's 5 s busy timeout when the
# backfill connection still holds a write transaction, so a locked run is
# always far slower than this budget.
_FAST_RUN_BUDGET_SEC = 2.0


def _backfill_events(db_path: Path) -> list[tuple[str, dict[str, object]]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT type, payload FROM events "
            "WHERE type LIKE 'events_backfill_%' ORDER BY timestamp, rowid"
        ).fetchall()
    return [(row[0], json.loads(row[1])) for row in rows]


def test_backfill_on_empty_db_is_fast_and_records_done_event(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"
    sqlite3.connect(db_path).close()

    started = time.perf_counter()
    stats = run_backfill(db_path)
    elapsed = time.perf_counter() - started

    assert elapsed < _FAST_RUN_BUDGET_SEC
    assert (stats.inserted, stats.attempted) == (0, 0)
    events = _backfill_events(db_path)
    assert [event_type for event_type, _ in events] == [
        "events_backfill_started",
        "events_backfill_done",
    ]
    assert events[1][1] == {"analytics_source": "events", "inserted": 0, "attempted": 0}
    with sqlite3.connect(db_path) as conn:
        state = conn.execute(
            "SELECT status FROM events_backfill_state WHERE id = 1"
        ).fetchone()
    assert state is not None and state[0] == "done"


def test_forced_backfill_rerun_is_fast_and_records_done_event(tmp_path: Path) -> None:
    db_path = tmp_path / "rerun.sqlite"
    sqlite3.connect(db_path).close()
    run_backfill(db_path)

    started = time.perf_counter()
    run_backfill(db_path, force=True)
    elapsed = time.perf_counter() - started

    assert elapsed < _FAST_RUN_BUDGET_SEC
    event_types = [event_type for event_type, _ in _backfill_events(db_path)]
    assert event_types.count("events_backfill_started") == 2
    assert event_types.count("events_backfill_done") == 2


def test_completed_backfill_is_skipped_without_events(tmp_path: Path) -> None:
    db_path = tmp_path / "skip.sqlite"
    sqlite3.connect(db_path).close()
    run_backfill(db_path)
    before = _backfill_events(db_path)

    run_backfill(db_path)

    assert _backfill_events(db_path) == before
