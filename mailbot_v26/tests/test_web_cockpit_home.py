import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import SupportMethod, SupportSettings, create_app
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

    def weekly_surprise_breakdown(
        self,
        account_email,
        *,
        since_ts,
        top_n,
        min_corrections,
        account_emails=None,
    ):
        return None

    def latest_trust_score_delta(self, *, limit=50):
        return None



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
        assert "a…@example.com" not in masked_body

        unmasked = client.get("/?account_emails=primary@example.com&pii=1")
        unmasked_body = unmasked.get_data(as_text=True)
        assert "alice@example.com" not in unmasked_body
        assert "a…@example.com" not in unmasked_body



def test_cockpit_home_survives_contacts_analytics_exceptions(tmp_path: Path) -> None:
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
        assert "Полезные ссылки" in body


def test_cockpit_home_shows_premium_support_card_when_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "support-enabled.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'href="/support"' in body
        assert "Support Letterbot" in body


def test_cockpit_home_top_nav_is_simplified(tmp_path: Path) -> None:
    db_path = tmp_path / "nav.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert ">Cockpit<" in body
    assert ">Archive<" in body
    assert ">Health<" in body
    assert ">Events<" in body
    assert ">Doctor<" in body
    assert ">Commitments<" not in body
    assert ">Latency<" not in body
    assert ">Attention<" not in body
    assert "class=\"nav-link active\">Cockpit<" in body
    assert ">Relationships<" not in body


def test_cockpit_home_quality_summary_safe_empty_state(tmp_path: Path) -> None:
    db_path = tmp_path / "quality-empty.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    app.config["ANALYTICS_FACTORY"] = lambda: FakeAnalytics()
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)
    assert "Качество автоматизации" in body
    assert "Not enough feedback yet." in body


def test_cockpit_home_quality_summary_with_data(tmp_path: Path) -> None:
    db_path = tmp_path / "quality-data.sqlite"
    KnowledgeDB(db_path)

    class QualityAnalytics(FakeAnalytics):
        def weekly_surprise_breakdown(
            self,
            account_email,
            *,
            since_ts,
            top_n,
            min_corrections,
            account_emails=None,
        ):
            return {"corrections": 4, "surprises": 1}

        def latest_trust_score_delta(self, *, limit=50):
            return {"delta": 0.2}

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    app.config["ANALYTICS_FACTORY"] = lambda: QualityAnalytics()
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/?account_emails=primary@example.com").get_data(as_text=True)

    assert "Качество автоматизации" in body
    assert "Corrections" in body
    assert ">4<" in body
    assert "25%" in body


def test_cockpit_home_shows_support_qr_preview_when_available(tmp_path: Path) -> None:
    db_path = tmp_path / "support-qr.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod(
                    "support",
                    "Поддержать Letterbot",
                    "",
                    "",
                    "",
                    "",
                    "support.png",
                    "data:image/png;base64,abc",
                )
            ],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Support QR" in body
        assert "<img" in body


def test_cockpit_home_hides_support_card_when_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / "support-disabled.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=False, show_in_nav=False, methods=[]),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'href="/support"' not in body
        assert "Support Letterbot" not in body


def test_footer_support_link_visibility(tmp_path: Path) -> None:
    db_path = tmp_path / "support-footer.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=True, show_in_nav=True, methods=[]),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert 'app-footer' in resp.get_data(as_text=True)
        assert 'href="/support"' in resp.get_data(as_text=True)

    app_hidden = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=False, show_in_nav=False, methods=[]),
    )
    with app_hidden.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert 'app-footer' in resp.get_data(as_text=True)
        assert 'href="/support"' not in resp.get_data(as_text=True)
