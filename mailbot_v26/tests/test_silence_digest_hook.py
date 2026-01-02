from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mailbot_v26.config_loader import (
    AccountConfig,
    BotConfig,
    GeneralConfig,
    KeysConfig,
    StorageConfig,
)
from mailbot_v26.pipeline import digest_scheduler
from mailbot_v26.events.emitter import EventEmitter as ContractEventEmitter
from mailbot_v26.storage.analytics import KnowledgeAnalytics
from mailbot_v26.storage.knowledge_db import KnowledgeDB


class DummyLogger:
    def info(self, event: str, **fields: object) -> None:
        return None

    def warning(self, event: str, **fields: object) -> None:
        return None

    def error(self, event: str, **fields: object) -> None:
        return None


def _write_config(path: Path, *, silence_mode: str) -> None:
    path.write_text(
        """
[features]
enable_daily_digest = true
enable_weekly_digest = false
enable_silence_as_signal = """
        + silence_mode
        + """

[daily_digest]
hour = 9
minute = 0
""",
        encoding="utf-8",
    )


def _build_config(tmp_path: Path) -> BotConfig:
    return BotConfig(
        general=GeneralConfig(
            check_interval=120,
            max_email_mb=15,
            max_attachment_mb=15,
            max_zip_uncompressed_mb=80,
            max_extracted_chars=50000,
            max_extracted_total_chars=120000,
            admin_chat_id="",
        ),
        accounts=[
            AccountConfig(
                account_id="acc",
                login="account@example.com",
                password="pass",
                host="",
                port=993,
                use_ssl=True,
                telegram_chat_id="chat",
            )
        ],
        keys=KeysConfig(
            telegram_bot_token="token",
            cf_account_id="",
            cf_api_token="",
        ),
        storage=StorageConfig(db_path=tmp_path / "mailbot.sqlite"),
    )


def _build_storage(tmp_path: Path) -> digest_scheduler.DigestStorage:
    db_path = tmp_path / "knowledge.sqlite"
    return digest_scheduler.DigestStorage(
        knowledge_db=KnowledgeDB(db_path),
        analytics=KnowledgeAnalytics(db_path),
        contract_event_emitter=ContractEventEmitter(db_path),
    )


def test_silence_scan_runs_when_flag_enabled(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, silence_mode="shadow")
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    calls: list[dict[str, object]] = []

    def _run(**kwargs) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(digest_scheduler, "run_silence_scan", _run)

    digest_scheduler.run_digest_tick(
        now=datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc),
        config=_build_config(tmp_path),
        storage=_build_storage(tmp_path),
        telegram_sender=lambda payload: None,
        logger=DummyLogger(),
    )

    assert len(calls) == 1


def test_silence_scan_skipped_when_flag_disabled(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.ini"
    _write_config(config_path, silence_mode="disabled")
    monkeypatch.setattr(digest_scheduler, "_CONFIG_PATH", config_path)

    calls: list[dict[str, object]] = []

    def _run(**kwargs) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(digest_scheduler, "run_silence_scan", _run)

    digest_scheduler.run_digest_tick(
        now=datetime(2025, 1, 6, 9, 1, tzinfo=timezone.utc),
        config=_build_config(tmp_path),
        storage=_build_storage(tmp_path),
        telegram_sender=lambda payload: None,
        logger=DummyLogger(),
    )

    assert calls == []
