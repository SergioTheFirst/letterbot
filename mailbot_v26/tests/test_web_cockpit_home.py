import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.observability.processing_span import ProcessingSpanRecorder
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app
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
            "engineer": (
                {
                    "slow_spans": [],
                    "recent_errors": [],
                    "latency_distribution": [],
                }
                if include_engineer
                else {}
            ),
        }

    def cockpit_top_senders(self, account_emails, days=30, limit=3):
        return []

    def cockpit_silent_contacts(
        self, account_emails, silent_days=14, days=90, min_msgs=3, limit=3
    ):
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


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str = "primary@example.com",
    email_id: int = 1,
    payload: dict[str, object] | None = None,
) -> None:
    payload_json = payload or {}
    conn.execute(
        """
        INSERT INTO events_v1 (
            event_type, ts_utc, ts, account_id, entity_id, email_id, payload, payload_json, schema_version, fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_type,
            ts_utc,
            datetime.fromtimestamp(ts_utc, tz=timezone.utc).isoformat(),
            account_id,
            "entity",
            email_id,
            json.dumps(payload_json),
            json.dumps(payload_json),
            1,
            f"{event_type}:{email_id}:{int(ts_utc)}",
        ),
    )


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
    return create_app(
        db_path=db_path, password="pw", secret_key="secret", allow_pii=allow_pii
    )


def _insert_health_snapshot(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    ts_utc: float,
    system_mode: str = "FULL",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO system_health_snapshots (
            snapshot_id, ts_utc, payload_json, gates_state, metrics_brief, system_mode
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            ts_utc,
            json.dumps({"system_mode": system_mode}, ensure_ascii=False),
            json.dumps({"db": "ok"}, ensure_ascii=False),
            json.dumps({"telegram_delivery_success_rate": 0.99}, ensure_ascii=False),
            system_mode,
        ),
    )


def _insert_processing_span(
    conn: sqlite3.Connection,
    *,
    span_id: str,
    ts_start_utc: float,
    ts_end_utc: float,
    account_id: str = "primary@example.com",
    email_id: int = 1,
    llm_provider: str = "gigachat",
    llm_model: str = "giga-pro",
    health_snapshot_id: str = "snap-1",
    fallback_used: int = 0,
    outcome: str = "ok",
    error_code: str = "",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO processing_spans (
            span_id, ts_start_utc, ts_end_utc, total_duration_ms, account_id, email_id,
            stage_durations_json, llm_provider, llm_model, llm_latency_ms, llm_quality_score,
            fallback_used, outcome, error_code, health_snapshot_id, delivery_mode,
            wait_budget_seconds, elapsed_to_first_send_ms, edit_applied
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            span_id,
            ts_start_utc,
            ts_end_utc,
            int(max((ts_end_utc - ts_start_utc) * 1000.0, 1.0)),
            account_id,
            email_id,
            json.dumps({"parse": 20, "llm": 180}, ensure_ascii=False),
            llm_provider,
            llm_model,
            180,
            0.91,
            int(fallback_used),
            outcome,
            error_code,
            health_snapshot_id,
            "direct",
            0,
            0,
            0,
        ),
    )


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
        assert 'data-testid="engineer-blocks"' not in basic_resp.get_data(as_text=True)

        engineer_resp = client.get("/?mode=engineer")
        assert engineer_resp.status_code == 200
        assert analytics.calls[-1] is True
        assert 'data-testid="engineer-blocks"' in engineer_resp.get_data(as_text=True)


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

        def cockpit_silent_contacts(
            self, account_emails, silent_days=14, days=90, min_msgs=3, limit=3
        ):
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
    assert ">Commitments<" in body
    assert ">Latency<" in body
    assert ">Attention<" in body
    assert ">Learning<" in body
    assert ">Relationships<" in body
    assert "<span>Commitments&nbsp;</span>" not in body
    assert "&nbsp;" not in body
    assert 'class="nav-link active">Cockpit<' in body


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


def test_cockpit_home_renders_initial_live_preview_from_dashboard_payload(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "homepage-live-preview.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (account_email, from_email, subject, received_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "primary@example.com",
                "alice@example.com",
                "Invoice",
                now.isoformat(),
                now.isoformat(),
            ),
        )
        _insert_event(
            conn,
            event_type="email_processed",
            ts_utc=now.timestamp(),
            payload={"text": "email processed"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/?account_emails=primary@example.com").get_data(as_text=True)

    assert 'id="preview-emails-today">1<' in body
    assert "Payload updated:" in body
    assert 'id="preview-recent-events"' in body
    assert "email processed" in body


def test_cockpit_home_preview_shows_live_payload_and_honest_no_data_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "homepage-no-data.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert 'id="preview-emails-today">0<' in body
    assert 'id="preview-dashboard-updated">Payload updated:' in body
    assert 'id="preview-dashboard-detail">' in body
    assert "No recent events yet." in body or "Unavailable:" in body
    assert 'data-testid="homepage-latency"' in body
    assert "NO LATENCY DATA" in body
    assert 'data-testid="homepage-health"' in body
    assert "UNKNOWN" in body
    assert 'data-testid="homepage-ai"' in body
    assert "NO AI DATA" in body


def test_cockpit_home_renders_latency_health_and_ai_from_runtime_data(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "homepage-observability.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO emails (id, account_email, from_email, subject, received_at, created_at, llm_provider)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "primary@example.com",
                "alice@example.com",
                "Invoice",
                now.isoformat(),
                now.isoformat(),
                "gigachat",
            ),
        )
        _insert_health_snapshot(
            conn,
            snapshot_id="snap-1",
            ts_utc=now.timestamp(),
        )
        _insert_processing_span(
            conn,
            span_id="span-1",
            ts_start_utc=(now - timedelta(milliseconds=250)).timestamp(),
            ts_end_utc=now.timestamp(),
        )
        _insert_event(
            conn,
            event_type="imap_health",
            ts_utc=now.timestamp(),
            payload={"subtype": "success"},
        )
        _insert_event(
            conn,
            event_type="message_interpretation",
            ts_utc=now.timestamp(),
            payload={"doc_kind": "invoice", "priority": "high"},
        )
        _insert_event(
            conn,
            event_type="telegram_delivered",
            ts_utc=now.timestamp(),
        )
        _insert_event(
            conn,
            event_type="DECISION_TRACE_RECORDED",
            ts_utc=now.timestamp(),
            payload={
                "decision_key": "trace-1",
                "decision_kind": "priority",
                "anchor_ts_utc": now.timestamp(),
                "signals_evaluated": ["INVOICE_KEYWORD", "AMOUNT_DUE"],
                "signals_fired": ["INVOICE_KEYWORD"],
                "evidence": {"matched": 2, "total": 3},
                "model_fingerprint": "model-1",
                "explain_codes": ["INVOICE_KEYWORD", "AMOUNT_DUE"],
                "trace_schema": "DecisionTraceV1",
                "trace_version": 1,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert 'data-testid="homepage-health"' in body
    assert "PARTIAL" in body
    assert "DB: OK" in body
    assert 'data-testid="homepage-latency"' in body
    assert "Pipeline p50" in body
    assert "1 spans" in body
    assert 'data-testid="homepage-ai"' in body
    assert "Provider: gigachat giga-pro" in body
    assert "priority: INVOICE_KEYWORD, AMOUNT_DUE" in body
    assert "1/1 (100%)" in body


def test_cockpit_home_health_block_marks_stale_runtime_as_unknown(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "homepage-health-stale.sqlite"
    KnowledgeDB(db_path)
    ProcessingSpanRecorder(db_path)
    old = datetime.now(timezone.utc) - timedelta(hours=3)
    with sqlite3.connect(db_path) as conn:
        _insert_health_snapshot(
            conn,
            snapshot_id="snap-stale",
            ts_utc=old.timestamp(),
        )
        _insert_processing_span(
            conn,
            span_id="span-stale",
            ts_start_utc=(old - timedelta(milliseconds=250)).timestamp(),
            ts_end_utc=old.timestamp(),
            health_snapshot_id="snap-stale",
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert 'data-testid="homepage-health"' in body
    assert "UNKNOWN" in body or "PARTIAL" in body
    assert '<li><span class="badge muted">IMAP: UNKNOWN</span>' in body


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
        support_settings=SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod(
                    "support",
                    "Support",
                    "",
                    "",
                    "",
                    "https://example.com/donate",
                    "",
                    "",
                )
            ],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert "app-footer" in resp.get_data(as_text=True)
        assert 'href="/support"' in resp.get_data(as_text=True)
        assert 'class="footer-donate-link"' in resp.get_data(as_text=True)
        assert 'href="https://example.com/donate"' in resp.get_data(as_text=True)

    app_hidden = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=False, show_in_nav=False, methods=[]),
    )
    with app_hidden.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        assert "app-footer" in resp.get_data(as_text=True)
        assert 'href="/support"' not in resp.get_data(as_text=True)
        assert 'class="footer-donate-link"' not in resp.get_data(as_text=True)


def test_cockpit_donate_surfaces_hidden_when_support_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / "donate-disabled.sqlite"
    KnowledgeDB(db_path)
    app = create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(enabled=False, show_in_nav=False, methods=[]),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert "donate-corner" not in body
    assert "donate-bottom-link" not in body
    assert "footer-donate-link" not in body


def test_cockpit_donate_surfaces_visible_when_support_enabled_with_url(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "donate-enabled.sqlite"
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
                    "Support",
                    "",
                    "",
                    "",
                    "https://example.com/donate",
                    "",
                    "",
                )
            ],
        ),
    )
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert "donate-corner" in body
    assert "donate-bottom-link" in body
    assert 'class="footer-donate-link"' in body
    assert 'href="https://example.com/donate"' in body


def test_cockpit_home_renders_default_cloudtips_donate_block_with_qr(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "homepage-cloudtips.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert 'data-testid="homepage-donate"' in body
    assert "https://pay.cloudtips.ru/p/00d77c6a" in body
    assert "Поддержать Letterbot" in body
    assert 'alt="QR-код для поддержки Letterbot"' in body
    assert 'src="data:image/png;base64,' in body


def test_cockpit_home_donate_block_gracefully_degrades_without_qr(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "homepage-cloudtips-no-qr.sqlite"
    missing_qr = tmp_path / "missing-qrcode.png"
    KnowledgeDB(db_path)
    monkeypatch.setattr(web_app, "DEFAULT_HOMEPAGE_DONATE_QR_PATH", missing_qr)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        login_with_csrf(client, "pw")
        body = client.get("/").get_data(as_text=True)

    assert 'data-testid="homepage-donate"' in body
    assert "https://pay.cloudtips.ru/p/00d77c6a" in body
    assert "QR-код недоступен" in body
    assert 'alt="QR-код для поддержки Letterbot"' not in body
