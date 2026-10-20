"""The Home engineer card shows the spans it is given (P3b D1/C10).

With engineer mode on and slow or failed processing spans recorded, the
card only said "Engineer mode enabled." and never listed them.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ACCOUNT = "primary@example.com"
ENGINEER_SCOPE = {"account_emails": ACCOUNT, "mode": "engineer"}


def _insert_span(
    conn: sqlite3.Connection,
    *,
    span_id: str,
    ts_start: float,
    duration_ms: int,
    outcome: str,
    error_code: str,
    provider: str,
    model: str,
) -> None:
    conn.execute(
        """
        INSERT INTO processing_spans (
            span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id,
            email_id, stage_durations_json, llm_provider, llm_model,
            llm_latency_ms, outcome, error_code, health_snapshot_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            span_id,
            ts_start,
            ts_start + duration_ms / 1000.0,
            duration_ms,
            ACCOUNT,
            1,
            json.dumps({"parse": 100, "llm": duration_ms - 100}),
            provider,
            model,
            duration_ms - 100,
            outcome,
            error_code,
            "",
        ),
    )


def _app(tmp_path: Path, *, with_spans: bool):
    db_path = tmp_path / "engineer-card.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    if with_spans:
        now = datetime.now(timezone.utc).timestamp()
        with sqlite3.connect(db_path) as conn:
            _insert_span(
                conn,
                span_id="span-slow",
                ts_start=now - 600,
                duration_ms=48250,
                outcome="ok",
                error_code="",
                provider="gigachat",
                model="giga-pro",
            )
            _insert_span(
                conn,
                span_id="span-error",
                ts_start=now - 300,
                duration_ms=1200,
                outcome="error",
                error_code="LLM_TIMEOUT",
                provider="cloudflare",
                model="llama-3",
            )
            conn.commit()
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _engineer_card(body: str) -> str:
    match = re.search(
        r'<section[^>]*data-testid="engineer-blocks"[^>]*>(.*?)</section>', body, re.S
    )
    assert match is not None, "engineer card missing"
    return match.group(1)


def test_engineer_card_lists_slow_spans_and_recent_errors(tmp_path: Path) -> None:
    app = _app(tmp_path, with_spans=True)
    with app.test_client() as client:
        body = client.get("/", query_string=ENGINEER_SCOPE).get_data(as_text=True)

    card = _engineer_card(body)
    assert "Engineer mode enabled." not in card
    assert "Slowest spans" in card
    assert "Recent errors" in card
    # Slowest first: the 48 s span heads the slow-span table.
    slow_rows = re.findall(r"<tr>(.*?)</tr>", card.split("Recent errors")[0], re.S)
    assert "48250" in slow_rows[1]
    assert "gigachat giga-pro" in slow_rows[1]
    assert "1200" in slow_rows[2]
    # The failed span shows its error code.
    error_part = card.split("Recent errors")[1]
    assert "LLM_TIMEOUT" in error_part
    assert "cloudflare llama-3" in error_part
    assert "giga-pro" not in error_part


def test_engineer_card_without_spans_says_so(tmp_path: Path) -> None:
    app = _app(tmp_path, with_spans=False)
    with app.test_client() as client:
        body = client.get("/", query_string=ENGINEER_SCOPE).get_data(as_text=True)

    card = _engineer_card(body)
    assert "No extra engineer-only samples in this scope." in card
    assert "<table" not in card


def test_basic_mode_has_no_engineer_card(tmp_path: Path) -> None:
    app = _app(tmp_path, with_spans=True)
    with app.test_client() as client:
        body = client.get(
            "/", query_string={"account_emails": ACCOUNT, "mode": "basic"}
        ).get_data(as_text=True)

    assert 'data-testid="engineer-blocks"' not in body
    assert "LLM_TIMEOUT" not in body
