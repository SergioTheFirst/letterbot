import logging

import mailbot_v26.start as start_module
from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)


class DummyState:
    def get_last_uid(self, login):
        return 0

    def get_last_check_time(self, login):
        return None

    def update_last_uid(self, login, uid):
        return None

    def update_check_time(self, login, timestamp=None):
        return None

    def set_imap_status(self, login, status, error=""):
        return None

    def save(self):
        return None


class DummyStorage:
    def __init__(self, db_path):
        self.db_path = db_path
        self._next_id = 0

    def upsert_email(
        self,
        account_email,
        uid,
        message_id,
        from_email,
        from_name,
        subject,
        received_at,
        attachments_count,
    ):
        self._next_id += 1
        return self._next_id

    def enqueue_stage(self, email_id, stage):
        return None

    def claim_next(self, stages):
        return None

    def mark_done(self, queue_id):
        return None

    def mark_error(self, queue_id, error, backoff):
        return None

    def close(self):
        return None


class DummyProcessor:
    def __init__(self, config, state):
        return None

    def process(self, login, inbound):
        return ""


class DummyHealthChecker:
    def __init__(self, base_config_dir, config):
        return None

    def run(self):
        return {}

    def evaluate_mode(self, results):
        return "FULL"


class DummyLaunchReportBuilder:
    def build(self, results, mode):
        return "report"


def _config(accounts, tmp_path):
    general = GeneralConfig(
        check_interval=1,
        max_email_mb=15,
        max_attachment_mb=1,
        max_zip_uncompressed_mb=80,
        max_extracted_chars=50_000,
        max_extracted_total_chars=120_000,
        admin_chat_id="admin",
    )
    keys = KeysConfig(telegram_bot_token="token", cf_account_id="cf", cf_api_token="cf_token")
    storage = StorageConfig(db_path=tmp_path / "mailbot.sqlite")
    return BotConfig(general=general, accounts=accounts, keys=keys, storage=storage)


def _install_common_patches(monkeypatch, accounts, tmp_path):
    monkeypatch.setattr(start_module, "load_config", lambda *_args, **_kwargs: _config(accounts, tmp_path))
    monkeypatch.setattr(start_module, "run_startup_mail_account_healthcheck", lambda *_args, **_kwargs: accounts)
    monkeypatch.setattr(start_module, "Storage", DummyStorage)
    monkeypatch.setattr(start_module, "StateManager", lambda *_args, **_kwargs: DummyState())
    monkeypatch.setattr(start_module, "MessageProcessor", DummyProcessor)
    monkeypatch.setattr(start_module, "configure_pipeline", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "run_self_check", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(start_module, "StartupHealthChecker", DummyHealthChecker)
    monkeypatch.setattr(start_module, "LaunchReportBuilder", DummyLaunchReportBuilder)
    monkeypatch.setattr(start_module, "dispatch_launch_report", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(start_module, "send_telegram", lambda *_args, **_kwargs: type("R", (), {"delivered": True})())
    original_runtime_health = start_module.AccountRuntimeHealthManager
    monkeypatch.setattr(
        start_module,
        "AccountRuntimeHealthManager",
        lambda *args, **kwargs: original_runtime_health(tmp_path / "runtime_health.json"),
    )
    monkeypatch.setattr(start_module, "time", start_module.time)
    monkeypatch.setattr(start_module.time, "sleep", lambda *_args, **_kwargs: None)


def _build_imap(responses, call_log=None):
    class FakeIMAP:
        def __init__(self, account, state, start_time, **_kwargs):
            self.login = account.login

        def fetch_new_messages(self):
            if call_log is not None:
                call_log.append(self.login)
            sequence = responses[self.login]
            if not sequence:
                return []
            result = sequence.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    return FakeIMAP


def _assert_cycle_logs(caplog):
    assert "Cycle #1 started" in caplog.text
    assert "Cycle #2 started" in caplog.text
    assert "Cycle #3 started" in caplog.text


def test_polling_loop_runs_three_cycles_on_empty_inbox(monkeypatch, tmp_path, caplog):
    account = AccountConfig(
        account_id="acc1",
        login="acc1@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat1",
    )
    responses = {account.login: [[], [], []]}
    monkeypatch.setattr(start_module, "ResilientIMAP", _build_imap(responses))
    _install_common_patches(monkeypatch, [account], tmp_path)

    caplog.set_level(logging.INFO, logger="mailbot")
    start_module.main(max_cycles=3)

    _assert_cycle_logs(caplog)


def test_polling_loop_continues_after_imap_exception(monkeypatch, tmp_path, caplog):
    account = AccountConfig(
        account_id="acc1",
        login="acc1@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat1",
    )
    responses = {
        account.login: [RuntimeError("boom"), [], []],
    }
    monkeypatch.setattr(start_module, "ResilientIMAP", _build_imap(responses))
    _install_common_patches(monkeypatch, [account], tmp_path)

    caplog.set_level(logging.INFO, logger="mailbot")
    start_module.main(max_cycles=3)

    _assert_cycle_logs(caplog)


def test_polling_loop_keeps_other_accounts_running(monkeypatch, tmp_path, caplog):
    bad_account = AccountConfig(
        account_id="bad",
        login="bad@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat1",
    )
    good_account = AccountConfig(
        account_id="good",
        login="good@example.com",
        password="pw",
        host="imap.example.com",
        port=993,
        use_ssl=True,
        telegram_chat_id="chat2",
    )
    responses = {
        bad_account.login: [RuntimeError("boom"), [], []],
        good_account.login: [[], [], []],
    }
    call_log = []
    monkeypatch.setattr(start_module, "ResilientIMAP", _build_imap(responses, call_log))
    _install_common_patches(monkeypatch, [bad_account, good_account], tmp_path)

    caplog.set_level(logging.INFO, logger="mailbot")
    start_module.main(max_cycles=3)

    _assert_cycle_logs(caplog)
    assert call_log.count(good_account.login) == 3
