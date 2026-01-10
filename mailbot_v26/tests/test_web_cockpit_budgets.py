import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _seed_budgets(db_path: Path, now: datetime) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO account_budgets (
                account_email, budget_type, limit_value, period, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "llm_tokens",
                1000,
                "YEARLY",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        for idx, consumed in enumerate([100, 200, 300], start=1):
            ts = (now - timedelta(days=idx)).isoformat()
            conn.execute(
                """
                INSERT INTO budget_consumption (
                    account_email, budget_type, consumed, reason, event_id, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "primary@example.com",
                    "llm_tokens",
                    consumed,
                    "test",
                    f"event-{idx}",
                    ts,
                ),
            )


def _seed_emails(db_path: Path, now: datetime) -> None:
    with sqlite3.connect(db_path) as conn:
        rows = [
            ("primary@example.com", "alice@example.com", "🔴", 0),
            ("primary@example.com", "bob@example.com", "🟡", 0),
            ("primary@example.com", "carol@example.com", "🔵", 0),
            ("primary@example.com", "dana@example.com", "🟡", 1),
        ]
        for idx, (account_email, from_email, priority, deferred) in enumerate(rows, start=1):
            conn.execute(
                """
                INSERT INTO emails (
                    id, account_email, from_email, priority, deferred_for_digest, received_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    idx,
                    account_email,
                    from_email,
                    priority,
                    deferred,
                    (now - timedelta(days=1)).isoformat(),
                ),
            )


def test_cockpit_budget_and_lane_endpoints(tmp_path: Path) -> None:
    db_path = tmp_path / "cockpit.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    _seed_budgets(db_path, now)
    _seed_emails(db_path, now)

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})

        budgets_resp = client.get(
            "/api/v1/cockpit/budgets",
            query_string={
                "account_emails": "primary@example.com",
                "window_days": "30",
                "days": "30",
            },
        )
        assert budgets_resp.status_code == 200
        budgets_body = budgets_resp.get_data(as_text=True)
        assert "primary@example.com" not in budgets_body

        budgets_data = budgets_resp.get_json()
        assert budgets_data["account_emails"] == ["p…@example.com"]
        totals = budgets_data["status"]["totals"]
        assert totals["limit"] == 1000
        assert totals["consumed"] == 600
        assert totals["remaining"] == 400
        assert round(float(totals["percent_used"]), 2) == 60.0
        trend_dates = [item["date"] for item in budgets_data["trend"]["trend"]]
        assert trend_dates == sorted(trend_dates)

        lanes_resp = client.get(
            "/api/v1/cockpit/lanes",
            query_string={"account_emails": "primary@example.com", "window_days": "30"},
        )
        assert lanes_resp.status_code == 200
        lanes_body = lanes_resp.get_data(as_text=True)
        assert "primary@example.com" not in lanes_body
        lanes_data = lanes_resp.get_json()
        assert lanes_data["account_emails"] == ["p…@example.com"]
        distribution = lanes_data["distribution"]
        assert distribution["urgent"] == 1
        assert distribution["normal"] == 2
        assert distribution["delegated"] == 2
        assert distribution["total"] == 4
