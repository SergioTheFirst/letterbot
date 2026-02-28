import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


class FakeAnalytics:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def cockpit_summary(
        self,
        *,
        account_emails,
        window_days,
        allow_pii,
        include_engineer=False,
        activity_limit=15,
    ):
        self.calls.append(bool(include_engineer))
        return {
            "status_strip": {
                "system_mode": "ok",
                "gates_state": {},
                "metrics_brief": {},
                "updated_ts_utc": None,
                "db_size_bytes": 1024,
            },
            "today_digest": {"counts": [], "items": []},
            "week_digest": {"counts": [], "items": []},
            "recent_activity": [],
            "golden_signals": {},
            "engineer": {
                "slow_spans": [],
                "recent_errors": [],
                "latency_distribution": [],
            }
            if include_engineer
            else {},
        }
    def cockpit_top_senders(self, account_emails, days=30, limit=3):
        return []

    def cockpit_silent_contacts(self, account_emails, silent_days=14, days=90, min_msgs=3, limit=3):
        return []

    def cockpit_stalled_threads(self, account_emails, days=30, limit=3):
        return []



def _build_app_with_email(tmp_path: Path, *, allow_pii: bool = False):
    db_path = tmp_path / "cockpit.sqlite"
    KnowledgeDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, action_line, body_summary, received_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "Review contract",
                "Quarterly numbers",
                datetime.now(timezone.utc).isoformat(),
            ),
        )
    return create_app(db_path=db_path, password="pw", secret_key="secret", allow_pii=allow_pii)


def test_cockpit_mode_gating(tmp_path: Path) -> None:
    db_path = tmp_path / "gating.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    analytics = FakeAnalytics()
    app.config["ANALYTICS_FACTORY"] = lambda: analytics
    with app.test_client() as client:
        login_with_csrf(client, "pw")

        basic_resp = client.get("/")
        assert basic_resp.status_code == 200
        assert analytics.calls == [False]
        assert "data-testid=\"engineer-blocks\"" not in basic_resp.get_data(as_text=True)

        engineer_resp = client.get("/?mode=engineer")
        assert engineer_resp.status_code == 200
        assert analytics.calls[-1] is True
        assert "data-testid=\"engineer-blocks\"" in engineer_resp.get_data(as_text=True)


def test_cockpit_pii_default_and_override(tmp_path: Path) -> None:
    app = _build_app_with_email(tmp_path, allow_pii=True)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        masked = client.get("/?account_emails=primary@example.com")
        masked_body = masked.get_data(as_text=True)
        assert "alice@example.com" not in masked_body
        assert "a…@example.com" in masked_body

        unmasked = client.get("/?account_emails=primary@example.com&pii=1")
        unmasked_body = unmasked.get_data(as_text=True)
        assert "alice@example.com" not in unmasked_body
        assert "a…@example.com" in unmasked_body



def test_cockpit_contacts_cards_survive_analytics_exceptions(tmp_path: Path) -> None:
    db_path = tmp_path / "errors.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    class BrokenAnalytics(FakeAnalytics):
        def cockpit_top_senders(self, account_emails, days=30, limit=3):
            raise RuntimeError("boom")

        def cockpit_silent_contacts(self, account_emails, silent_days=14, days=90, min_msgs=3, limit=3):
            raise RuntimeError("boom")

        def cockpit_stalled_threads(self, account_emails, days=30, limit=3):
            raise RuntimeError("boom")

    app.config["ANALYTICS_FACTORY"] = lambda: BrokenAnalytics()
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Контакты" in body
        assert "— нет данных —" in body
