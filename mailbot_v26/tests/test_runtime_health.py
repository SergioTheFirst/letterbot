from datetime import datetime, timedelta, timezone

from mailbot_v26.config_loader import AccountConfig
from mailbot_v26.mail_health.runtime_health import AccountRuntimeHealthManager


def _account(account_id: str = "acc1") -> AccountConfig:
    return AccountConfig(
        account_id=account_id,
        login=f"{account_id}@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat",
    )


def _manager(tmp_path, cooldown_minutes: int = 60) -> AccountRuntimeHealthManager:
    mgr = AccountRuntimeHealthManager(tmp_path / "runtime_state.json", alert_cooldown_minutes=cooldown_minutes)
    mgr.register_account(_account())
    return mgr


def test_backoff_grows(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    expected_minutes = [1, 5, 15, 60, 360]
    for minutes in expected_minutes:
        mgr.on_failure("acc1", RuntimeError("boom"), now)
        state = mgr.get_state("acc1")
        assert state.next_retry_at_utc == now + timedelta(minutes=minutes)


def test_should_skip_when_backoff_active(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mgr.on_failure("acc1", RuntimeError("boom"), now)
    assert mgr.should_attempt("acc1", now + timedelta(seconds=30)) is False
    assert mgr.should_attempt("acc1", now + timedelta(minutes=2)) is True


def test_dedupe_same_error_within_cooldown(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    should_alert, _ = mgr.on_failure("acc1", RuntimeError("boom"), now)
    assert should_alert is True
    later = now + timedelta(minutes=30)
    should_alert_repeat, _ = mgr.on_failure("acc1", RuntimeError("boom"), later)
    assert should_alert_repeat is False


def test_alert_when_fingerprint_changes(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mgr.on_failure("acc1", RuntimeError("boom"), now)
    should_alert, _ = mgr.on_failure("acc1", ValueError("boom"), now + timedelta(minutes=1))
    assert should_alert is True


def test_persistence_survives_restart(tmp_path):
    path = tmp_path / "runtime_state.json"
    mgr = AccountRuntimeHealthManager(path)
    mgr.register_account(_account())
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mgr.on_failure("acc1", RuntimeError("boom"), now)

    restored = AccountRuntimeHealthManager(path)
    restored.register_account(_account())
    state = restored.get_state("acc1")
    assert state.consecutive_failures == 1
    assert state.next_retry_at_utc is not None


def test_other_accounts_continue(tmp_path):
    mgr = AccountRuntimeHealthManager(tmp_path / "runtime_state.json")
    bad = _account("bad")
    good = _account("good")
    mgr.register_account(bad)
    mgr.register_account(good)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    mgr.on_failure("bad", RuntimeError("boom"), now)
    assert mgr.should_attempt("bad", now) is False
    assert mgr.should_attempt("good", now) is True

def test_imap_repeated_handshake_failure_enters_24h_cooldown(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    for i in range(3):
        mgr.on_failure("acc1", TimeoutError("SSL handshake timed out"), now + timedelta(minutes=i))

    state = mgr.get_state("acc1")
    assert state.cooldown_until_utc is not None
    assert state.next_retry_at_utc == state.cooldown_until_utc
    assert state.cooldown_reason == "repeated_handshake_connect_failures"
    assert state.next_retry_at_utc == now + timedelta(hours=24, minutes=2)


def test_imap_single_transient_failure_does_not_enter_day_cooldown(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    mgr.on_failure("acc1", TimeoutError("connect timed out"), now)

    state = mgr.get_state("acc1")
    assert state.cooldown_until_utc is None
    assert state.next_retry_at_utc == now + timedelta(minutes=1)


def test_imap_cooldown_suppresses_minutely_retry_spam(tmp_path):
    mgr = _manager(tmp_path)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    should_alert_1, _ = mgr.on_failure("acc1", TimeoutError("SSL handshake timed out"), now)
    should_alert_2, _ = mgr.on_failure("acc1", TimeoutError("SSL handshake timed out"), now + timedelta(minutes=1))
    should_alert_3, _ = mgr.on_failure("acc1", TimeoutError("SSL handshake timed out"), now + timedelta(minutes=2))

    assert should_alert_1 is True
    assert should_alert_2 is False
    assert should_alert_3 is False

    state = mgr.get_state("acc1")
    assert state.cooldown_until_utc is not None
    assert mgr.should_attempt("acc1", now + timedelta(minutes=30)) is False
    assert mgr.should_attempt("acc1", state.cooldown_until_utc + timedelta(minutes=1)) is True
