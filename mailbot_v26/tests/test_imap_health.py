from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import mailbot_v26.start as start_module
from mailbot_v26.bot_core.pipeline import PipelineContext
from mailbot_v26.bot_core.storage import Storage
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.events.contract import EventType
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.state_manager import StateManager
from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import login_with_csrf
from mailbot_v26.web_observability.app import create_app


def _account() -> AccountConfig:
    return AccountConfig(
        account_id="acc1",
        login="primary@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


def _config(tmp_path: Path, *, db_name: str = "imap-health.sqlite") -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=1,
            max_email_mb=15,
            max_attachment_mb=1,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50_000,
            max_extracted_total_chars=120_000,
            admin_chat_id="",
        ),
        accounts=[_account()],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="cf",
            cf_api_token="cf-token",
        ),
        storage=StorageConfig(db_path=tmp_path / db_name),
    )


def _insert_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    ts_utc: float,
    account_id: str = "primary@example.com",
    payload: dict[str, object] | None = None,
) -> None:
    body = json.dumps(payload or {}, ensure_ascii=False)
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
            1,
            body,
            body,
            1,
            f"{event_type}-{ts_utc}-{body}",
        ),
    )


def _seed_parse_queue(
    tmp_path: Path,
    *,
    uid: int = 1,
    subject: str = "Subject",
) -> tuple[Storage, BotConfig, int]:
    config = _config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = storage.upsert_email(
        account_email=_account().login,
        uid=uid,
        message_id=f"<{uid}@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject=subject,
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
    )
    storage.enqueue_stage(email_id, "PARSE")
    start_module.PIPELINE_CACHE[email_id] = PipelineContext(
        email_id=email_id,
        account_email=_account().login,
        uid=uid,
    )
    return storage, config, email_id


def _reset_pipeline_cache() -> None:
    start_module.PIPELINE_CACHE.clear()
    start_module.PIPELINE_INBOUND_CACHE.clear()
    start_module.PIPELINE_RAW_CACHE.clear()


def test_processing_failure_does_not_crash_polling_loop(
    monkeypatch, tmp_path: Path
) -> None:
    _reset_pipeline_cache()
    storage, config, email_id = _seed_parse_queue(tmp_path)
    monkeypatch.setattr(
        start_module.processor_module,
        "contract_event_emitter",
        ContractEventEmitter(config.storage.db_path),
    )
    monkeypatch.setattr(
        start_module, "stage_parse", lambda _ctx: (_ for _ in ()).throw(RuntimeError("parse boom"))
    )
    fail_open_calls: list[int] = []
    monkeypatch.setattr(
        start_module,
        "_fail_open_process",
        lambda *_args, **_kwargs: fail_open_calls.append(email_id),
    )

    start_module._process_queue(
        storage,
        config,
        SimpleNamespace(),
        SimpleNamespace(ENABLE_PREMIUM_PROCESSOR=False),
    )

    row = storage.conn.execute(
        "SELECT attempts, last_error FROM queue WHERE email_id = ? AND stage = 'PARSE'",
        (email_id,),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1
    assert "parse boom" in str(row[1] or "")
    assert fail_open_calls == []


def test_processing_failure_writes_health_event(
    monkeypatch, tmp_path: Path
) -> None:
    _reset_pipeline_cache()
    storage, config, email_id = _seed_parse_queue(tmp_path)
    monkeypatch.setattr(
        start_module.processor_module,
        "contract_event_emitter",
        ContractEventEmitter(config.storage.db_path),
    )
    monkeypatch.setattr(
        start_module, "stage_parse", lambda _ctx: (_ for _ in ()).throw(RuntimeError("parse boom"))
    )
    monkeypatch.setattr(start_module, "_fail_open_process", lambda *_args, **_kwargs: None)

    start_module._process_queue(
        storage,
        config,
        SimpleNamespace(),
        SimpleNamespace(ENABLE_PREMIUM_PROCESSOR=False),
    )

    with sqlite3.connect(config.storage.db_path) as conn:
        row = conn.execute(
            """
            SELECT payload_json
            FROM events_v1
            WHERE event_type = ?
            ORDER BY ts_utc DESC
            LIMIT 1
            """,
            (EventType.IMAP_HEALTH.value,),
        ).fetchone()
    assert row is not None
    payload = json.loads(str(row[0]))
    assert payload["subtype"] == "processing_failure"
    assert payload["stage"] == "PARSE"
    assert payload["attempt_count"] == 1


def test_dead_letter_after_max_retries(monkeypatch, tmp_path: Path) -> None:
    _reset_pipeline_cache()
    storage, config, email_id = _seed_parse_queue(tmp_path, uid=7)
    storage.conn.execute(
        "UPDATE queue SET attempts = 2 WHERE email_id = ? AND stage = 'PARSE'",
        (email_id,),
    )
    storage.conn.commit()
    monkeypatch.setattr(
        start_module.processor_module,
        "contract_event_emitter",
        ContractEventEmitter(config.storage.db_path),
    )
    monkeypatch.setattr(
        start_module, "stage_parse", lambda _ctx: (_ for _ in ()).throw(RuntimeError("parse boom"))
    )
    fail_open_calls: list[int] = []
    monkeypatch.setattr(
        start_module,
        "_fail_open_process",
        lambda *_args, **_kwargs: fail_open_calls.append(email_id),
    )

    start_module._process_queue(
        storage,
        config,
        SimpleNamespace(),
        SimpleNamespace(ENABLE_PREMIUM_PROCESSOR=False),
    )

    queue_row = storage.conn.execute(
        "SELECT 1 FROM queue WHERE email_id = ?",
        (email_id,),
    ).fetchone()
    email_row = storage.conn.execute(
        "SELECT status, error_last FROM emails WHERE id = ?",
        (email_id,),
    ).fetchone()
    with sqlite3.connect(config.storage.db_path) as conn:
        payloads = [
            json.loads(str(row[0]))
            for row in conn.execute(
                "SELECT payload_json FROM events_v1 WHERE event_type = ? ORDER BY ts_utc",
                (EventType.IMAP_HEALTH.value,),
            ).fetchall()
        ]
    assert queue_row is None
    assert email_row is not None
    assert email_row[0] == "ERROR"
    assert "parse boom" in str(email_row[1] or "")
    assert fail_open_calls == [email_id]
    assert [item["subtype"] for item in payloads][-2:] == [
        "processing_failure",
        "dead_letter",
    ]


def test_dead_letter_uid_skipped_on_next_poll(tmp_path: Path) -> None:
    _reset_pipeline_cache()
    config = _config(tmp_path)
    storage = Storage(config.storage.db_path)
    email_id = storage.upsert_email(
        account_email=_account().login,
        uid=55,
        message_id="<55@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Broken",
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
    )
    storage.set_email_error(email_id, "dead letter")

    duplicate_id, enqueued = start_module._persist_inbound_and_enqueue_parse(
        storage=storage,
        account_email=_account().login,
        uid=55,
        message_id="<55@example.com>",
        from_email="sender@example.com",
        from_name="Sender",
        subject="Broken",
        received_at=datetime.now(timezone.utc).isoformat(),
        attachments_count=0,
        raw_email=b"raw",
        inbound=SimpleNamespace(subject="Broken", attachments=[], sender="sender@example.com"),
    )

    queue_count = storage.conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    assert duplicate_id == email_id
    assert enqueued is False
    assert queue_count == 0


def test_reconnect_writes_health_event(monkeypatch, tmp_path: Path) -> None:
    config = _config(tmp_path, db_name="reconnect.sqlite")
    responses = [RuntimeError("boom"), []]

    class FastRetryHealthManager(AccountRuntimeHealthManager):
        @staticmethod
        def _calculate_backoff_minutes(consecutive_failures: int) -> int:
            _ = consecutive_failures
            return 0

    class FakeIMAP:
        def __init__(self, _account, _state, _start_time, **_kwargs):
            self.last_fetch_included_prestart = False
            self.last_bootstrap_active = False
            self.last_uidvalidity_changed = False
            self.last_resync_reason = "normal_poll"

        def fetch_new_messages(self):
            result = responses.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    monkeypatch.setattr(start_module, "load_config", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(
        start_module, "run_startup_mail_account_healthcheck", lambda *_args, **_kwargs: [config.accounts[0]]
    )
    monkeypatch.setattr(start_module, "ResilientIMAP", FakeIMAP)
    monkeypatch.setattr(
        start_module, "StateManager", lambda *_args, **_kwargs: StateManager(tmp_path / "state.json")
    )
    monkeypatch.setattr(
        start_module,
        "AccountRuntimeHealthManager",
        lambda *_args, **_kwargs: FastRetryHealthManager(tmp_path / "runtime_health.json"),
    )
    monkeypatch.setattr(start_module, "MessageProcessor", lambda *args, **kwargs: SimpleNamespace(config=config))
    monkeypatch.setattr(start_module, "configure_pipeline", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "require_runtime_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "validate_dist_runtime", lambda **_kwargs: (True, ""))
    monkeypatch.setattr(start_module, "run_self_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "maybe_backfill_events", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "dispatch_launch_report", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(start_module, "run_inbound_polling", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "run_digest_tick", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        start_module,
        "send_telegram",
        lambda *_args, **_kwargs: type("R", (), {"delivered": True, "message_id": 1})(),
    )
    monkeypatch.setattr(start_module.time, "sleep", lambda *_args, **_kwargs: None)

    class HealthyChecker:
        def __init__(self, *_args, **_kwargs):
            return None

        def run(self):
            return []

        def evaluate_mode(self, _results):
            return "FULL"

    monkeypatch.setattr(start_module, "StartupHealthChecker", HealthyChecker)
    monkeypatch.setattr(
        start_module,
        "LaunchReportBuilder",
        lambda *args, **kwargs: SimpleNamespace(build=lambda *_a, **_k: "report"),
    )

    start_module.main(config_dir=tmp_path, max_cycles=2)

    with sqlite3.connect(config.storage.db_path) as conn:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM events_v1
            WHERE event_type = ?
            ORDER BY ts_utc
            """,
            (EventType.IMAP_HEALTH.value,),
        ).fetchall()
    subtypes = [json.loads(str(row[0]))["subtype"] for row in rows]
    assert "reconnect" in subtypes


def test_uidvalidity_change_triggers_safe_resync_and_health_event(
    monkeypatch, tmp_path: Path
) -> None:
    config = _config(tmp_path, db_name="uidvalidity.sqlite")

    class FakeIMAP:
        def __init__(self, _account, _state, _start_time, **_kwargs):
            self.last_fetch_included_prestart = False
            self.last_bootstrap_active = True
            self.last_uidvalidity_changed = True
            self.last_resync_reason = "uidvalidity_change"

        def fetch_new_messages(self):
            return []

    monkeypatch.setattr(start_module, "load_config", lambda *_args, **_kwargs: config)
    monkeypatch.setattr(
        start_module, "run_startup_mail_account_healthcheck", lambda *_args, **_kwargs: [config.accounts[0]]
    )
    monkeypatch.setattr(start_module, "ResilientIMAP", FakeIMAP)
    monkeypatch.setattr(
        start_module, "StateManager", lambda *_args, **_kwargs: StateManager(tmp_path / "state.json")
    )
    monkeypatch.setattr(
        start_module,
        "AccountRuntimeHealthManager",
        lambda *_args, **_kwargs: AccountRuntimeHealthManager(tmp_path / "runtime_health.json"),
    )
    monkeypatch.setattr(start_module, "MessageProcessor", lambda *args, **kwargs: SimpleNamespace(config=config))
    monkeypatch.setattr(start_module, "configure_pipeline", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "require_runtime_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "validate_dist_runtime", lambda **_kwargs: (True, ""))
    monkeypatch.setattr(start_module, "run_self_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "maybe_backfill_events", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "dispatch_launch_report", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(start_module, "run_inbound_polling", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "run_digest_tick", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        start_module,
        "send_telegram",
        lambda *_args, **_kwargs: type("R", (), {"delivered": True, "message_id": 1})(),
    )
    monkeypatch.setattr(start_module.time, "sleep", lambda *_args, **_kwargs: None)

    class HealthyChecker:
        def __init__(self, *_args, **_kwargs):
            return None

        def run(self):
            return []

        def evaluate_mode(self, _results):
            return "FULL"

    monkeypatch.setattr(start_module, "StartupHealthChecker", HealthyChecker)
    monkeypatch.setattr(
        start_module,
        "LaunchReportBuilder",
        lambda *args, **kwargs: SimpleNamespace(build=lambda *_a, **_k: "report"),
    )

    start_module.main(config_dir=tmp_path, max_cycles=1)

    with sqlite3.connect(config.storage.db_path) as conn:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM events_v1
            WHERE event_type = ?
            ORDER BY ts_utc
            """,
            (EventType.IMAP_HEALTH.value,),
        ).fetchall()
    payloads = [json.loads(str(row[0])) for row in rows]
    uidvalidity_events = [
        item for item in payloads if item.get("subtype") == "uidvalidity_change"
    ]
    assert uidvalidity_events
    assert uidvalidity_events[-1]["resync_reason"] == "uidvalidity_change"


def test_health_api_imap_returns_correct_status(tmp_path: Path) -> None:
    db_path = tmp_path / "health-api-imap.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type=EventType.IMAP_HEALTH.value,
            ts_utc=(now - timedelta(minutes=2)).timestamp(),
            payload={"subtype": "success", "detail": "ok"},
        )
        _insert_event(
            conn,
            event_type=EventType.IMAP_HEALTH.value,
            ts_utc=(now - timedelta(hours=1)).timestamp(),
            payload={"subtype": "reconnect", "detail": "recovered"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/imap")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["reconnect_count_24h"] == 1
    assert data["dead_letter_count"] == 0
    assert data["last_success_ts"]


def test_health_api_pipeline_returns_correct_status(tmp_path: Path) -> None:
    db_path = tmp_path / "health-api-pipeline.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type=EventType.MESSAGE_INTERPRETATION.value,
            ts_utc=now.timestamp(),
            payload={
                "doc_kind": "invoice",
                "amount": 12000,
                "sender_email": "vendor@example.com",
                "action": "Проверить",
                "priority": "🟡",
                "confidence": 0.9,
            },
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/health/pipeline")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["processing_failure_count_24h"] == 0
    assert data["pending_action_count"] == 1
    assert data["last_processed_ts"]


def test_health_events_not_read_by_semantic_pipeline(tmp_path: Path) -> None:
    db_path = tmp_path / "health-events-semantic.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        _insert_event(
            conn,
            event_type=EventType.IMAP_HEALTH.value,
            ts_utc=now.timestamp(),
            payload={"subtype": "dead_letter", "detail": "parse failed"},
        )
        conn.commit()

    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["interpretation"] == {
        "invoice_count": 0,
        "contract_count": 0,
        "invoice_total": 0,
    }
    assert data["business"]["payable_amount_total"] == 0
    assert data["business"]["documents_waiting_attention_count"] == 0
