import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mailbot_v26.insights.commitment_tracker import Commitment
from mailbot_v26.pipeline.processor import _build_priority_signal_text
from mailbot_v26.priority.priority_engine_v2 import (
    PriorityEngineV2,
    PriorityV2Config,
    VipSenderMatcher,
    load_priority_v2_config,
    load_vip_senders,
)
from mailbot_v26.storage.analytics import KnowledgeAnalytics


def _init_events_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("""
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                entity_id TEXT,
                email_id TEXT,
                payload JSON
            );
            """)
        conn.commit()


def _insert_email_event(
    conn: sqlite3.Connection,
    *,
    ts: datetime,
    from_email: str,
    subject: str,
    mail_type: str,
) -> None:
    payload = json.dumps(
        {
            "from_email": from_email,
            "subject": subject,
            "mail_type": mail_type,
        },
        ensure_ascii=False,
    )
    conn.execute(
        """
        INSERT INTO events (id, type, timestamp, entity_id, email_id, payload)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, "email_received", ts.isoformat(), None, None, payload),
    )


def _engine_for(path: Path, vip_patterns: tuple[str, ...] = ()) -> PriorityEngineV2:
    analytics = KnowledgeAnalytics(path)
    return PriorityEngineV2(
        analytics,
        config=PriorityV2Config(),
        vip_senders=VipSenderMatcher(vip_patterns),
    )


def test_urgency_weighted_by_type(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="Срочно оплатить",
        body_text="",
        from_email="finance@example.com",
        mail_type="INVOICE",
        received_at=now,
        commitments=[],
    )

    assert result.score == 30
    assert "PRIO_URGENT_KEYWORD" in result.reason_codes
    assert "PRIO_URGENT_WEIGHTED_BY_TYPE" in result.reason_codes


def test_amount_thresholds(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="Счет на 12 000 руб",
        body_text="",
        from_email="billing@example.com",
        mail_type="INVOICE",
        received_at=now,
        commitments=[],
    )

    assert result.score == 10
    assert "PRIO_AMOUNT_10K" in result.reason_codes


def test_attachment_text_contributes_to_priority_score(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    signal_body = _build_priority_signal_text(
        "Нейтральное письмо",
        [
            {
                "filename": "table.xlsx",
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "text": "Итого: 87 500 руб. Оплатить до 15.04.2026",
            }
        ],
    )

    result = engine.compute(
        subject="Документы",
        body_text=signal_body,
        from_email="billing@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.score >= 20
    assert "PRIO_AMOUNT_50K" in result.reason_codes


def test_deadline_thresholds(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    commitments = [
        Commitment(
            commitment_text="Оплатить счет",
            deadline_iso=(now.date() + timedelta(days=1)).isoformat(),
            status="pending",
            source="heuristic",
            confidence=0.9,
        )
    ]

    result = engine.compute(
        subject="",
        body_text="",
        from_email="finance@example.com",
        mail_type="INVOICE",
        received_at=now,
        commitments=commitments,
    )

    assert result.score == 30
    assert "PRIO_DEADLINE_1D" in result.reason_codes


def test_vip_multiplier_fyi_dampen(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path, vip_patterns=("vip@example.com",))
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="FYI update",
        body_text="",
        from_email="vip@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.score == 6
    assert "PRIO_VIP_BASE" in result.reason_codes
    assert "PRIO_VIP_FYI_DAMPEN" in result.reason_codes


def test_vip_multiplier_frequency_dampen(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for i in range(28):
            _insert_email_event(
                conn,
                ts=now - timedelta(hours=i),
                from_email="vip@example.com",
                subject="Update",
                mail_type="UNKNOWN",
            )
        for i in range(2):
            _insert_email_event(
                conn,
                ts=now - timedelta(days=10 + i),
                from_email="vip@example.com",
                subject="Update",
                mail_type="UNKNOWN",
            )
        conn.commit()

    engine = _engine_for(db_path, vip_patterns=("vip@example.com",))
    result = engine.compute(
        subject="Update",
        body_text="",
        from_email="vip@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.score == 25
    assert "PRIO_VIP_BASE" in result.reason_codes
    assert "PRIO_VIP_FREQ_DAMPEN" in result.reason_codes
    assert "PRIO_FREQ_SPIKE_3X" in result.reason_codes


def test_vip_multiplier_commitment_boost(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path, vip_patterns=("vip@example.com",))
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="",
        body_text="",
        from_email="vip@example.com",
        mail_type="PAYMENT_REMINDER",
        received_at=now,
        commitments=[],
    )

    assert result.score == 30
    assert "PRIO_VIP_BASE" in result.reason_codes
    assert "PRIO_VIP_COMMITMENT_BOOST" in result.reason_codes


def test_frequency_anomaly_from_events(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    with sqlite3.connect(db_path) as conn:
        for i in range(28):
            _insert_email_event(
                conn,
                ts=now - timedelta(hours=i),
                from_email="sales@example.com",
                subject="Update",
                mail_type="UNKNOWN",
            )
        for i in range(2):
            _insert_email_event(
                conn,
                ts=now - timedelta(days=10 + i),
                from_email="sales@example.com",
                subject="Update",
                mail_type="UNKNOWN",
            )
        conn.commit()

    engine = _engine_for(db_path)
    result = engine.compute(
        subject="Update",
        body_text="",
        from_email="sales@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.score == 15
    assert "PRIO_FREQ_SPIKE_3X" in result.reason_codes


def test_chain_length_scoring(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_email_event(
            conn,
            ts=now - timedelta(days=3),
            from_email="billing@example.com",
            subject="Reminder notice",
            mail_type="PAYMENT_REMINDER",
        )
        _insert_email_event(
            conn,
            ts=now - timedelta(days=5),
            from_email="billing@example.com",
            subject="Reminder notice",
            mail_type="PAYMENT_REMINDER",
        )
        conn.commit()

    engine = _engine_for(db_path)
    result = engine.compute(
        subject="Reminder notice",
        body_text="",
        from_email="billing@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.score == 10
    assert "PRIO_CHAIN_2PLUS" in result.reason_codes


def test_deterministic_output(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result_a = engine.compute(
        subject="Срочно",
        body_text="",
        from_email="ops@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )
    result_b = engine.compute(
        subject="Срочно",
        body_text="",
        from_email="ops@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result_a.score == result_b.score
    assert result_a.reason_codes == result_b.reason_codes


def test_malformed_config_ini_falls_back_with_actionable_warning(
    tmp_path: Path, caplog
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text("check_interval=120\n", encoding="utf-8")
    (config_dir / "config.ini.example").write_text("[priority_v2]\n", encoding="utf-8")

    caplog.set_level("WARNING")
    config = load_priority_v2_config(config_dir)
    vip = load_vip_senders(config_dir)

    assert config == PriorityV2Config()
    assert vip == VipSenderMatcher()
    assert "config.ini is invalid" in caplog.text
    assert "config.ini.example" in caplog.text
    assert "Windows command: copy" in caplog.text
    assert "MissingSectionHeaderError" not in caplog.text


def test_missing_config_ini_uses_deterministic_defaults(tmp_path: Path, caplog) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    caplog.set_level("WARNING")
    config = load_priority_v2_config(config_dir)
    vip = load_vip_senders(config_dir)

    assert config == PriorityV2Config()
    assert vip == VipSenderMatcher()
    assert "config.ini missing" in caplog.text
    assert "Windows command: copy" in caplog.text


def test_invoice_subject_with_excel_attachment_signal_is_not_low_priority(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="Счет на оплату №42",
        body_text="invoice_42.xlsx excel attachment",
        from_email="finance@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.priority in {"🟡", "🔴"}
    assert "PRIO_INVOICE_SUBJECT" in result.reason_codes
    assert "PRIO_INVOICE_EXCEL_ATTACHMENT" in result.reason_codes


def test_tech_security_alert_gets_bounded_priority_uplift(tmp_path: Path) -> None:
    db_path = tmp_path / "priority_v2.sqlite"
    _init_events_db(db_path)
    engine = _engine_for(db_path)
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)

    result = engine.compute(
        subject="Важное оповещение системы безопасности",
        body_text="Подозрительный вход: устройство offline, не удаётся подключиться",
        from_email="security@example.com",
        mail_type="UNKNOWN",
        received_at=now,
        commitments=[],
    )

    assert result.priority in {"🟡", "🔴"}
    assert "PRIO_TECH_SECURITY_ALERT_STRONG" in result.reason_codes
