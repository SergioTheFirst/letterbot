"""WP2a ingest: commit the IMAP cursor only after persist + enqueue.

The worker loop commits the cursor only for UIDs that are durably stored
(new or duplicate), dead-letters a UID after three failed polls with an
IMAP_HEALTH event, and sends at most one aggregated alert per account per
24 hours (D-P2-1).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mailbot_v26.start as start_module
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
from mailbot_v26.imap_client import ResilientIMAP
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager
from mailbot_v26.pipeline.processor import InboundMessage
from mailbot_v26.state_manager import StateManager
from mailbot_v26.tests.test_imap_client import _FakeIMAPClient, _install_fake
from mailbot_v26.tests.test_storage_atomic_persist import (
    clear_pipeline_caches,
    persist,
    row_counts,
)

START = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
LOGIN = "primary@example.com"
POISON_SUBJECT = "Quarterly secret numbers"


def _account() -> AccountConfig:
    return AccountConfig(
        account_id="acc1",
        login=LOGIN,
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


def _config(tmp_path: Path) -> BotConfig:
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
        storage=StorageConfig(db_path=tmp_path / "ingest.sqlite"),
    )


def _messages(uids) -> dict[int, dict[bytes, object]]:
    return {
        uid: {
            b"RFC822": b"m%d" % uid,
            b"RFC822.SIZE": 10,
            b"INTERNALDATE": START + timedelta(minutes=uid),
        }
        for uid in uids
    }


@pytest.fixture(autouse=True)
def _clean_pipeline_cache():
    clear_pipeline_caches()
    yield
    clear_pipeline_caches()


def test_crash_between_persist_and_cursor_commit_creates_no_duplicate(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.set_messages(_messages([1, 2]))
    _install_fake(monkeypatch, fake)
    state_path = tmp_path / "state.json"
    state = StateManager(state_path)
    state.update_check_time(LOGIN)
    state.save(force=True)
    storage = Storage(tmp_path / "db.sqlite")

    first = ResilientIMAP(_account(), state, start_time=START).fetch_new_messages(
        is_known_uid=start_module._known_uid_probe(storage, LOGIN)
    )
    for uid, _raw in first:
        persist(storage, uid)
    # crash: neither commit_cursor() nor state.save() ran
    storage.close()

    restarted_state = StateManager(state_path)
    restarted_storage = Storage(tmp_path / "db.sqlite")
    fake.fetch_calls.clear()
    client = ResilientIMAP(_account(), restarted_state, start_time=START)
    second = client.fetch_new_messages(
        is_known_uid=start_module._known_uid_probe(restarted_storage, LOGIN)
    )
    commit = client.commit_cursor([uid for uid, _ in second])

    assert [uid for uid, _ in first] == [1, 2]
    assert second == []
    assert [f for f in fake.fetch_calls if b"RFC822" in f[1]] == []
    assert commit.cursor == 2
    assert row_counts(restarted_storage.db_path) == (2, 2)


# ---- worker loop (start.main) ----


def _install_main(monkeypatch, tmp_path: Path, fake: _FakeIMAPClient):
    config = _config(tmp_path)
    sent: list[str] = []
    queue_runs: list[int] = []

    def _parse(raw: bytes, _config):
        if POISON_SUBJECT.encode("ascii") in raw:
            raise ValueError("broken MIME")
        return InboundMessage(subject="Subject", body="Body", sender="s@example.com")

    def _send(payload, *_args, **_kwargs):
        sent.append(payload.html_text)
        return SimpleNamespace(delivered=True, message_id=1)

    class _HealthyChecker:
        def __init__(self, *_args, **_kwargs):
            return None

        def run(self):
            return []

        def evaluate_mode(self, _results):
            return "FULL"

    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(start_module, "RUN_STARTED_AT_UTC", START)
    monkeypatch.setattr(start_module, "load_config", lambda *_a, **_k: config)
    monkeypatch.setattr(
        start_module,
        "run_startup_mail_account_healthcheck",
        lambda *_a, **_k: [config.accounts[0]],
    )
    monkeypatch.setattr(
        start_module, "StateManager", lambda *_a, **_k: StateManager(tmp_path / "state.json")
    )
    monkeypatch.setattr(
        start_module,
        "AccountRuntimeHealthManager",
        lambda *_a, **_k: AccountRuntimeHealthManager(tmp_path / "runtime_health.json"),
    )
    monkeypatch.setattr(
        start_module, "MessageProcessor", lambda *_a, **_k: SimpleNamespace(config=config)
    )
    monkeypatch.setattr(start_module, "configure_pipeline", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "require_runtime_for", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "validate_dist_runtime", lambda **_k: (True, ""))
    monkeypatch.setattr(start_module, "run_self_check", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "maybe_backfill_events", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "dispatch_launch_report", lambda *_a, **_k: True)
    monkeypatch.setattr(start_module, "run_inbound_polling", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "run_digest_tick", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "send_telegram", _send)
    monkeypatch.setattr(start_module, "parse_raw_email", _parse)
    monkeypatch.setattr(
        start_module, "_process_queue", lambda *_a, **_k: queue_runs.append(1)
    )
    monkeypatch.setattr(start_module.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(start_module, "StartupHealthChecker", _HealthyChecker)
    monkeypatch.setattr(
        start_module,
        "LaunchReportBuilder",
        lambda *_a, **_k: SimpleNamespace(build=lambda *_x, **_y: "report"),
    )
    monkeypatch.setattr(
        start_module.processor_module,
        "contract_event_emitter",
        ContractEventEmitter(config.storage.db_path),
    )
    # not a first run: the account already has a cursor
    seeded = StateManager(tmp_path / "state.json")
    seeded.update_check_time(LOGIN)
    seeded.save(force=True)
    return config, sent


def _saved_state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))


def _imap_health_payloads(db_path: Path) -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT payload_json FROM events_v1 WHERE event_type = ? ORDER BY id",
            (EventType.IMAP_HEALTH.value,),
        ).fetchall()
    return [json.loads(str(row[0])) for row in rows]


def test_main_commits_cursor_after_persist_and_saves_state(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.set_messages(_messages([1, 3]))
    config, sent = _install_main(monkeypatch, tmp_path, fake)

    start_module.main(config_dir=tmp_path, max_cycles=1)

    assert _saved_state(tmp_path)["accounts"][LOGIN]["last_uid"] == 3
    assert row_counts(config.storage.db_path) == (2, 2)
    assert sent == []


def test_main_commits_cursor_when_poll_returns_nothing(
    monkeypatch, tmp_path: Path
) -> None:
    # all mail predates the process start: policy-skipped, cursor still moves
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    fake.set_messages(
        {
            uid: {b"RFC822": b"old", b"INTERNALDATE": START - timedelta(hours=uid)}
            for uid in (4, 5)
        }
    )
    config, _sent = _install_main(monkeypatch, tmp_path, fake)

    start_module.main(config_dir=tmp_path, max_cycles=1)

    assert _saved_state(tmp_path)["accounts"][LOGIN]["last_uid"] == 5
    assert row_counts(config.storage.db_path) == (0, 0)


def test_main_dead_letters_unparseable_uid_after_three_polls_with_one_alert(
    monkeypatch, tmp_path: Path
) -> None:
    fake = _FakeIMAPClient(host="imap.example.com", port=993, ssl=True)
    messages = _messages([1, 2, 3])
    messages[2][b"RFC822"] = (
        f"From: sender@example.com\r\nSubject: {POISON_SUBJECT}\r\n\r\nbody"
    ).encode("ascii")
    fake.set_messages(messages)
    config, sent = _install_main(monkeypatch, tmp_path, fake)

    start_module.main(config_dir=tmp_path, max_cycles=5)

    saved = _saved_state(tmp_path)
    assert saved["accounts"][LOGIN]["last_uid"] == 3
    assert saved["uid_attempts"].get(LOGIN, {}) == {}
    assert row_counts(config.storage.db_path) == (2, 2)

    dead = [
        payload
        for payload in _imap_health_payloads(config.storage.db_path)
        if payload["subtype"] == "dead_letter"
    ]
    assert len(dead) == 1
    assert dead[0]["stage"] == "FETCH"
    assert dead[0]["message_uid"] == 2
    assert dead[0]["attempt_count"] == 3

    assert len(sent) == 1
    alert = sent[0]
    assert "after 3 attempts: 1" in alert or "после 3 попыток: 1" in alert
    assert POISON_SUBJECT not in alert
    assert "sender@example.com" not in alert
    assert LOGIN not in alert


def test_dead_letter_alert_is_sent_at_most_once_per_account_per_day(
    monkeypatch, tmp_path: Path
) -> None:
    sent: list[str] = []

    def _send(payload, *_args, **_kwargs):
        sent.append(payload.html_text)
        return SimpleNamespace(delivered=True, message_id=1)

    monkeypatch.setattr(start_module, "send_telegram", _send)
    state_path = tmp_path / "state.json"
    state = StateManager(state_path)
    account = _account()

    def _alert(state: StateManager, now: datetime) -> bool:
        return start_module._maybe_send_dead_letter_alert(
            state=state,
            account=account,
            login=LOGIN,
            dead_count=2,
            bot_token="token",
            now_utc=now,
        )

    assert _alert(state, START) is True
    assert _alert(state, START + timedelta(hours=23)) is False
    state.save(force=True)
    # the 24 h window survives a restart
    restarted = StateManager(state_path)
    assert _alert(restarted, START + timedelta(hours=23, minutes=59)) is False
    assert _alert(restarted, START + timedelta(hours=24)) is True

    assert len(sent) == 2
    assert all(
        "after 3 attempts: 2" in text or "после 3 попыток: 2" in text for text in sent
    )


def test_undelivered_dead_letter_alert_is_retried_next_time(
    monkeypatch, tmp_path: Path
) -> None:
    results = [False, True]

    def _send(payload, *_args, **_kwargs):
        return SimpleNamespace(delivered=results.pop(0), message_id=None)

    monkeypatch.setattr(start_module, "send_telegram", _send)
    state = StateManager(tmp_path / "state.json")

    def _alert(now: datetime) -> bool:
        return start_module._maybe_send_dead_letter_alert(
            state=state,
            account=_account(),
            login=LOGIN,
            dead_count=1,
            bot_token="token",
            now_utc=now,
        )

    assert _alert(START) is False
    assert _alert(START + timedelta(minutes=5)) is True
    assert results == []
