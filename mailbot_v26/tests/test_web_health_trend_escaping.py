"""Health trend cells are HTML-escaped exactly once (P3b D1/C8).

The Gates/Metrics summaries were escaped in Python and then again by the
template, so a quote showed up as "&#x27;" on the page.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"


def _app(tmp_path: Path):
    db_path = tmp_path / "health-trend.sqlite"
    ProcessingSpanRecorder(db_path)
    ts = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    gates = {"db": "ok", "note": "<b>bold</b>"}
    metrics = {"24h": {"telegram_delivery_success_rate": 0.99}}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO system_health_snapshots (snapshot_id, ts_utc, payload_json,
                                                 gates_state, metrics_brief, system_mode)
            VALUES ('snap-1', ?, '{}', ?, ?, 'FULL')
            """,
            (ts, json.dumps(gates), json.dumps(metrics)),
        )
        conn.execute(
            """
            INSERT INTO processing_spans (span_id, ts_start_utc, ts_end_utc,
                                          total_duration_ms, account_id, outcome,
                                          health_snapshot_id)
            VALUES ('span-1', ?, ?, 100, ?, 'ok', 'snap-1')
            """,
            (ts - 1, ts, ACCOUNT),
        )
        conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_health_trend_cells_are_escaped_once(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        body = client.get(
            "/health", query_string={"account_emails": ACCOUNT, "mode": "engineer"}
        ).get_data(as_text=True)

    cells = re.findall(r'<td class="truncate">(.*?)</td>', body, re.S)
    assert len(cells) >= 4  # Gates + Metrics in the trend and engineer tables
    for cell in cells:
        assert "&amp;" not in cell, cell
        # Still escaped once: no markup from the snapshot reaches the page.
        assert "<b>" not in cell
    text = " ".join(html.unescape(cell) for cell in cells)
    assert "'telegram_delivery_success_rate': 0.99" in text
    assert "note: <b>bold</b>" in text
