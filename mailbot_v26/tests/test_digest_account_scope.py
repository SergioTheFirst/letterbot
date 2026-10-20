"""B8: a chat's digest shows, and is triggered by, only that chat's accounts."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from mailbot_v26 import config_loader
from mailbot_v26.account_identity import normalize_login
from mailbot_v26.events.contract import EventType
from mailbot_v26.insights.anomaly_engine import Anomaly
from mailbot_v26.pipeline import daily_digest, weekly_digest
from mailbot_v26.tests._digest_helpers import (
    MSK,
    ScriptedSender,
    account,
    bot_config,
    build_storage,
    emit_score_pair,
    seed_deferred,
    seed_interaction,
    tick,
    write_config,
)

ALPHA = "alpha@example.com"
ALPHA_SECOND = "second@example.com"
BETA = "beta@example.com"
DAILY_DUE = datetime(2026, 10, 20, 9, 5, tzinfo=MSK)
MONDAY_DUE = datetime(2026, 10, 19, 9, 5, tzinfo=MSK)
TRUST_MARKERS = ("Trust level", "Уровень доверия")
HEALTH_MARKERS = ("Relationship health", "Здоровье отношений")


def _two_chats(tmp_path):
    return bot_config(tmp_path, [account(ALPHA, "chat-a"), account(BETA, "chat-b")])


def _relationship_changes(storage, *, account_id: str, entity_id: str) -> None:
    emit_score_pair(
        storage,
        account_id=account_id,
        entity_id=entity_id,
        event_type=EventType.TRUST_SCORE_UPDATED,
        field="trust_score",
        before=0.2,
        after=0.6,
    )
    emit_score_pair(
        storage,
        account_id=account_id,
        entity_id=entity_id,
        event_type=EventType.RELATIONSHIP_HEALTH_UPDATED,
        field="health_score",
        before=40.0,
        after=70.0,
    )


def _texts_by_chat(sender: ScriptedSender) -> dict[str, str]:
    return {
        str(payload.metadata["chat_id"]): payload.html_text
        for payload in sender.payloads
    }


def _always_anomalous(**_kwargs) -> list[Anomaly]:
    return [Anomaly(type="FREQUENCY_DROP", severity="WARN", title="Drop", details="")]


def test_weekly_trust_deltas_are_limited_to_the_given_accounts(tmp_path) -> None:
    storage = build_storage(tmp_path)
    _relationship_changes(storage, account_id=BETA, entity_id="beta-partner")
    _relationship_changes(storage, account_id=ALPHA, entity_id="alpha-partner")

    scoped = storage.analytics.weekly_trust_score_deltas(
        days=7, account_emails=[ALPHA]
    )
    unscoped = storage.analytics.weekly_trust_score_deltas(days=7)

    assert [item["entity_id"] for item in scoped["up"] + scoped["down"]] == [
        "alpha-partner"
    ]
    assert {item["entity_id"] for item in unscoped["up"]} == {
        "alpha-partner",
        "beta-partner",
    }
    assert storage.analytics.weekly_trust_score_deltas(
        days=7, account_emails=[]
    ) == {"up": [], "down": []}


def test_latest_trust_and_health_deltas_are_limited_to_the_given_accounts(
    tmp_path,
) -> None:
    storage = build_storage(tmp_path)
    _relationship_changes(storage, account_id=BETA, entity_id="beta-partner")
    analytics = storage.analytics

    assert analytics.latest_trust_score_delta(account_emails=[ALPHA]) is None
    assert analytics.latest_relationship_health_delta(account_emails=[ALPHA]) is None
    trust = analytics.latest_trust_score_delta(account_emails=[BETA])
    health = analytics.latest_relationship_health_delta(account_emails=[BETA])
    assert trust is not None and trust["entity_id"] == "beta-partner"
    assert health is not None and health["entity_id"] == "beta-partner"
    # Unscoped callers (the web cockpit) keep the global view.
    unscoped = analytics.latest_trust_score_delta()
    assert unscoped is not None and unscoped["entity_id"] == "beta-partner"


def test_recent_entity_activity_counts_only_the_given_accounts_emails(
    tmp_path,
) -> None:
    storage = build_storage(tmp_path)
    seed_interaction(storage, account_id=ALPHA, entity_id="alpha-partner")
    for _ in range(2):
        seed_interaction(storage, account_id=BETA, entity_id="beta-partner")
    analytics = storage.analytics

    scoped = analytics.recent_entity_activity(
        days=30, limit=5, account_emails=[ALPHA]
    )
    unscoped = analytics.recent_entity_activity(days=30, limit=5)

    assert [(row["entity_id"], row["total"]) for row in scoped] == [
        ("alpha-partner", 1)
    ]
    assert [row["entity_id"] for row in unscoped] == ["beta-partner", "alpha-partner"]
    assert analytics.recent_entity_activity(days=30, limit=5, account_emails=[]) == []


def test_another_accounts_relationship_changes_send_this_chat_no_daily_digest(
    monkeypatch, tmp_path
) -> None:
    write_config(tmp_path, monkeypatch, daily=True, weekly=False)
    storage = build_storage(tmp_path)
    _relationship_changes(storage, account_id=BETA, entity_id="beta-partner")
    sender = ScriptedSender()

    tick(_two_chats(tmp_path), storage, sender, DAILY_DUE)

    assert sender.chats == ["chat-b"]


def test_daily_digest_shows_no_trust_or_health_line_of_another_account(
    monkeypatch, tmp_path
) -> None:
    write_config(tmp_path, monkeypatch, daily=True, weekly=False)
    storage = build_storage(tmp_path)
    seed_deferred(storage, ALPHA)
    _relationship_changes(storage, account_id=BETA, entity_id="beta-partner")
    sender = ScriptedSender()

    tick(_two_chats(tmp_path), storage, sender, DAILY_DUE)

    texts = _texts_by_chat(sender)
    assert set(texts) == {"chat-a", "chat-b"}
    for marker in TRUST_MARKERS + HEALTH_MARKERS:
        assert marker not in texts["chat-a"]
    assert any(marker in texts["chat-b"] for marker in TRUST_MARKERS)


def test_another_accounts_relationship_changes_send_this_chat_no_weekly_digest(
    monkeypatch, tmp_path
) -> None:
    write_config(tmp_path, monkeypatch, daily=False, weekly=True)
    storage = build_storage(tmp_path)
    _relationship_changes(storage, account_id=BETA, entity_id="beta-partner")
    sender = ScriptedSender()

    tick(_two_chats(tmp_path), storage, sender, MONDAY_DUE)

    assert sender.chats == ["chat-b"]


def test_anomaly_alerts_and_events_name_only_the_chats_own_entities(
    monkeypatch, tmp_path
) -> None:
    write_config(
        tmp_path,
        monkeypatch,
        daily=True,
        weekly=False,
        extra="enable_anomaly_alerts = true\n",
    )
    storage = build_storage(tmp_path)
    seed_interaction(storage, account_id=ALPHA, entity_id="alpha-partner")
    seed_interaction(storage, account_id=BETA, entity_id="beta-partner")
    monkeypatch.setattr(daily_digest, "compute_anomalies", _always_anomalous)
    sender = ScriptedSender()

    tick(_two_chats(tmp_path), storage, sender, DAILY_DUE)

    texts = _texts_by_chat(sender)
    assert "alpha-partner" in texts["chat-a"]
    assert "beta-partner" not in texts["chat-a"]
    assert "beta-partner" in texts["chat-b"]
    with sqlite3.connect(storage.knowledge_db.path) as conn:
        rows = conn.execute(
            "SELECT account_id, entity_id FROM events_v1 WHERE event_type = ?",
            (EventType.ANOMALY_DETECTED.value,),
        ).fetchall()
    assert sorted(rows) == [(ALPHA, "alpha-partner"), (BETA, "beta-partner")]


def test_weekly_anomaly_alerts_count_only_the_chats_own_entities(
    monkeypatch, tmp_path
) -> None:
    write_config(
        tmp_path,
        monkeypatch,
        daily=False,
        weekly=True,
        extra="enable_anomaly_alerts = true\n",
    )
    storage = build_storage(tmp_path)
    seed_interaction(storage, account_id=BETA, entity_id="beta-partner")
    monkeypatch.setattr(weekly_digest, "compute_anomalies", _always_anomalous)
    sender = ScriptedSender()

    tick(_two_chats(tmp_path), storage, sender, MONDAY_DUE)

    # Beta's anomaly is weekly content for chat B only.
    assert sender.chats == ["chat-b"]


def test_digest_collectors_get_the_accounts_routed_to_the_chat(
    monkeypatch, tmp_path
) -> None:
    write_config(tmp_path, monkeypatch, daily=True, weekly=True)
    storage = build_storage(tmp_path)
    seen: dict[tuple[str, str], object] = {}

    def _spy(kind: str, real):
        def _collect(**kwargs):
            seen[(kind, kwargs["account_email"])] = kwargs.get("account_emails")
            return real(**kwargs)

        return _collect

    monkeypatch.setattr(
        daily_digest,
        "_collect_digest_data",
        _spy("daily", daily_digest._collect_digest_data),
    )
    monkeypatch.setattr(
        weekly_digest,
        "_collect_weekly_data",
        _spy("weekly", weekly_digest._collect_weekly_data),
    )
    config = bot_config(
        tmp_path,
        [
            account(ALPHA, "chat-a"),
            account(ALPHA_SECOND, "chat-a"),
            account(BETA, "chat-b"),
        ],
    )

    tick(config, storage, ScriptedSender(), MONDAY_DUE)

    assert seen == {
        ("daily", ALPHA): [ALPHA, ALPHA_SECOND],
        ("daily", BETA): [BETA],
        ("weekly", ALPHA): [ALPHA, ALPHA_SECOND],
        ("weekly", BETA): [BETA],
    }


# --- logins as configured vs. normalize_login(login) as stored --------------

MIXED_CASE = "Owner@Example.COM"


def test_mixed_case_login_finds_rows_stored_under_the_normalized_login(
    monkeypatch, tmp_path
) -> None:
    write_config(tmp_path, monkeypatch, daily=True, weekly=False)
    storage = build_storage(tmp_path)
    # Ingest stores normalize_login(login).
    seed_deferred(storage, normalize_login(MIXED_CASE))
    sender = ScriptedSender()

    tick(
        bot_config(tmp_path, [account(MIXED_CASE, "chat-o")]),
        storage,
        sender,
        DAILY_DUE,
    )

    assert sender.chats == ["chat-o"]
    # digest_state and the sent event keep the login as configured.
    assert storage.knowledge_db.get_last_digest_sent_at(account_email=MIXED_CASE)
    with sqlite3.connect(storage.knowledge_db.path) as conn:
        account_id, payload_json = conn.execute(
            "SELECT account_id, payload_json FROM events_v1 WHERE event_type = ?",
            (EventType.DAILY_DIGEST_SENT.value,),
        ).fetchone()
    assert account_id == MIXED_CASE
    assert json.loads(payload_json)["account_emails"] == [MIXED_CASE]


def test_digest_scope_holds_both_login_forms(monkeypatch, tmp_path) -> None:
    write_config(tmp_path, monkeypatch, daily=False, weekly=True)
    storage = build_storage(tmp_path)
    seen: list[object] = []
    real = weekly_digest._collect_weekly_data

    def _collect(**kwargs):
        seen.append(kwargs.get("account_emails"))
        return real(**kwargs)

    monkeypatch.setattr(weekly_digest, "_collect_weekly_data", _collect)
    config = bot_config(
        tmp_path, [account(MIXED_CASE, "chat-o"), account(ALPHA_SECOND, "chat-o")]
    )

    tick(config, storage, ScriptedSender(), MONDAY_DUE)

    assert seen == [[MIXED_CASE, "owner@example.com", ALPHA_SECOND]]


def test_account_scope_from_accounts_ini_holds_both_login_forms(tmp_path) -> None:
    (tmp_path / "accounts.ini").write_text(
        "[boss]\n"
        f"login = {MIXED_CASE}\n"
        "password = pass\n"
        "telegram_chat_id = chat-o\n"
        "\n"
        "[deputy]\n"
        f"login = {ALPHA_SECOND}\n"
        "password = pass\n"
        "telegram_chat_id = chat-o\n",
        encoding="utf-8",
    )
    config_loader._load_account_scopes.cache_clear()
    try:
        scope = config_loader.resolve_account_scope(
            "owner@example.com", base_dir=tmp_path
        )
    finally:
        config_loader._load_account_scopes.cache_clear()

    assert scope is not None
    assert scope.chat_id == "chat-o"
    assert scope.account_emails == [MIXED_CASE, "owner@example.com", ALPHA_SECOND]
