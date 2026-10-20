import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from mailbot_v26.config_loader import load_config
from mailbot_v26.system.startup_health import (
    HealthStatus,
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
from mailbot_v26.text.mojibake import normalize_mojibake_text
from mailbot_v26.ui.branding import WATERMARK_LINE
from mailbot_v26.worker import telegram_sender


def _write_config_files(tmp_path, *, gigachat_enabled: bool, gigachat_key: str) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.ini").write_text(
        "\n".join(
            [
                "[general]",
                "check_interval = 180",
                "max_attachment_mb = 10",
                "admin_chat_id = 123",
                "",
                "[storage]",
                f"db_path = {tmp_path / 'mailbot.sqlite'}",
                "",
                "[llm]",
                "primary = cloudflare",
                "fallback = cloudflare",
                "",
                "[gigachat]",
                f"enabled = {'true' if gigachat_enabled else 'false'}",
                f"api_key = {gigachat_key}",
                "",
                "[cloudflare]",
                "enabled = true",
                "",
                "[llm_safety]",
                "gigachat_max_consecutive_errors = 3",
                "gigachat_max_latency_sec = 10",
                "gigachat_cooldown_sec = 600",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "\n".join(
            [
                "[account1]",
                "login = test@example.com",
                "password = secret",
                "host = imap.example.com",
                "port = 993",
                "use_ssl = true",
                "telegram_chat_id = 123",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "keys.ini").write_text(
        "\n".join(
            [
                "[telegram]",
                "bot_token = token",
                "",
                "[cloudflare]",
                "account_id = account",
                "api_token = token",
            ]
        ),
        encoding="utf-8",
    )


def test_startup_health_checker_never_raises(tmp_path, monkeypatch) -> None:
    _write_config_files(tmp_path, gigachat_enabled=False, gigachat_key="")
    config_dir = tmp_path / "config"
    config = load_config(config_dir)
    monkeypatch.setattr(telegram_sender, "requests", None)
    checker = StartupHealthChecker(config_dir, config)
    results = checker.run()
    assert isinstance(results, list)
    assert {item["component"] for item in results} >= {
        "Python",
        "OS",
        "DB",
        "Telegram",
        "GigaChat",
        "Cloudflare",
        "LLM Direct",
    }


def test_gigachat_unavailable_cloudflare_ok(tmp_path, monkeypatch) -> None:
    _write_config_files(tmp_path, gigachat_enabled=True, gigachat_key="")
    config_dir = tmp_path / "config"
    config = load_config(config_dir)
    monkeypatch.setattr(telegram_sender, "requests", None)
    checker = StartupHealthChecker(config_dir, config)
    results = {item["component"]: item for item in checker.run()}
    assert results["GigaChat"]["status"] in {HealthStatus.FAILED, HealthStatus.DEGRADED}
    assert results["Cloudflare"]["status"] == HealthStatus.OK


def test_launch_report_deterministic() -> None:
    builder = LaunchReportBuilder(version_label="LetterBot.ru v26")
    results = [
        {"component": "Cloudflare", "status": HealthStatus.OK, "details": "active"},
        {
            "component": "GigaChat",
            "status": HealthStatus.DEGRADED,
            "details": "disabled",
        },
        {"component": "DB", "status": HealthStatus.OK, "details": "/db"},
        {"component": "OS", "status": HealthStatus.OK, "details": "Linux"},
        {"component": "Python", "status": HealthStatus.OK, "details": "3.11"},
        {"component": "Telegram", "status": HealthStatus.OK, "details": "reachable"},
    ]
    report_a = builder.build(results, mode=SimpleNamespace(value="FULL"))
    report_b = builder.build(
        list(reversed(results)), mode=SimpleNamespace(value="FULL")
    )
    assert report_a == report_b


@pytest.fixture
def wired_llm_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    # The per-email LLM stage is not wired yet (WP5a / owner decision D1):
    # simulate a wired stage to test the provider-based delivery modes.
    monkeypatch.setattr(
        "mailbot_v26.pipeline.stage_llm.llm_stage_disabled_reason", lambda: None
    )


def test_launch_report_says_ai_is_off_while_the_llm_stage_is_unwired(
    tmp_path,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        "[llm]\nprimary = cloudflare\n\n[cloudflare]\nenabled = true\n",
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "[cloudflare]\naccount_id = account\napi_token = token\n",
        encoding="utf-8",
    )

    report = LaunchReportBuilder(config_dir=config_dir).build(
        results=[
            {
                "component": "LLM Direct",
                "status": HealthStatus.OK,
                "details": "configured + capability probe passed",
            }
        ],
        mode=SimpleNamespace(value="FULL"),
    )

    # A configured provider that passes its probe is not used per email while
    # the stage is unwired: the report must not claim direct AI summaries.
    assert "LLM delivery mode: DISABLED (llm_stage_not_wired)" in report
    assert "Immediate TG summaries: heuristic" in report


@pytest.mark.usefixtures("wired_llm_stage")
def test_launch_report_includes_honest_llm_delivery_mode(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        "\n".join(
            [
                "[llm]",
                "primary = cloudflare",
                "",
                "[cloudflare]",
                "enabled = true",
                "",
                "[llm_queue]",
                "llm_request_queue_enabled = false",
                "max_concurrent_llm_calls = 1",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "\n".join(
            [
                "[cloudflare]",
                "account_id = account",
                "api_token = token",
            ]
        ),
        encoding="utf-8",
    )

    report = LaunchReportBuilder(config_dir=config_dir).build(
        results=[
            {
                "component": "LLM Direct",
                "status": HealthStatus.FAILED,
                "details": "configured + capability probe failed",
            }
        ],
        mode=SimpleNamespace(value="FULL"),
    )

    assert "LLM delivery mode: HEURISTIC_FALLBACK" in report
    assert "Immediate TG summaries: heuristic" in report
    assert "background queue: disabled" in report
    assert "direct path check: configured + capability probe failed" in report


def test_startup_health_checker_probe_failure_is_non_blocking(
    tmp_path, monkeypatch
) -> None:
    _write_config_files(tmp_path, gigachat_enabled=False, gigachat_key="")
    config_dir = tmp_path / "config"
    config = load_config(config_dir)

    class _BoomProvider:
        def healthcheck(self) -> bool:
            raise RuntimeError("probe failure")

    def _build_providers(_self, _cfg):
        return {"cloudflare": _BoomProvider()}

    monkeypatch.setattr(
        "mailbot_v26.system.startup_health.llm_router.LLMRouter._build_providers",
        _build_providers,
    )

    checker = StartupHealthChecker(config_dir, config)
    results = {item["component"]: item for item in checker.run()}

    assert "LLM Direct" in results
    assert results["LLM Direct"]["status"] == HealthStatus.FAILED


@pytest.mark.usefixtures("wired_llm_stage")
def test_launch_report_marks_direct_delivery_mode(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        "\n".join(
            [
                "[llm]",
                "primary = cloudflare",
                "",
                "[cloudflare]",
                "enabled = true",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "\n".join(
            [
                "[cloudflare]",
                "account_id = account",
                "api_token = token",
            ]
        ),
        encoding="utf-8",
    )

    report = LaunchReportBuilder(config_dir=config_dir).build(
        results=[
            {
                "component": "LLM Direct",
                "status": HealthStatus.OK,
                "details": "configured + capability probe passed",
            }
        ],
        mode=SimpleNamespace(value="FULL"),
    )

    assert "LLM delivery mode: DIRECT" in report
    assert "Immediate TG summaries: direct" in report


def test_launch_report_marks_placeholder_llm_credentials_not_configured(tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.ini").write_text(
        "\n".join(
            [
                "[llm]",
                "primary = cloudflare",
                "",
                "[cloudflare]",
                "enabled = true",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "accounts.ini").write_text(
        "\n".join(
            [
                "[cloudflare]",
                "account_id = CHANGE_ME",
                "api_token = CHANGE_ME",
            ]
        ),
        encoding="utf-8",
    )

    report = LaunchReportBuilder(config_dir=config_dir).build(
        results=[
            {
                "component": "Cloudflare",
                "status": HealthStatus.DEGRADED,
                "details": "NOT CONFIGURED",
            },
            {
                "component": "LLM Direct",
                "status": HealthStatus.DEGRADED,
                "details": "NOT CONFIGURED",
            },
        ],
        mode=SimpleNamespace(value="FULL"),
    )

    assert "Cloudflare: DEGRADED (NOT CONFIGURED)" in report
    assert "direct path check: NOT CONFIGURED" in report
    assert "LLM delivery mode: DISABLED" in report
    assert "Immediate TG summaries: heuristic" in report


def test_startup_report_default_branding_and_watermark() -> None:
    report = LaunchReportBuilder().build(results=[], mode=SimpleNamespace(value="FULL"))
    assert "LetterBot.ru" in report
    assert "MailBot Premium" not in report
    assert WATERMARK_LINE in report


def test_no_mojibake_in_startup_report() -> None:
    report = LaunchReportBuilder().build(results=[], mode=SimpleNamespace(value="FULL"))

    assert normalize_mojibake_text(report) == report
    for token in ("Р Р†Р вЂљ", "Р В РЎвЂўР РЋРІР‚С™", "РЎР‚РЎСџ", "вЂ"):
        assert token not in report


def test_startup_report_includes_mail_account_status_ok() -> None:
    builder = LaunchReportBuilder(version_label="LetterBot.ru v26")
    report = builder.build(
        results=[],
        mode=SimpleNamespace(value="FULL"),
        mail_accounts=[{"account_id": "mos_ru", "status": "OK", "error": ""}],
    )

    assert "Mail accounts:" in report
    assert "- mos_ru: OK" in report


def test_startup_report_includes_mail_account_status_failed() -> None:
    builder = LaunchReportBuilder(version_label="LetterBot.ru v26")
    report = builder.build(
        results=[],
        mode=SimpleNamespace(value="FULL"),
        mail_accounts=[
            {
                "account_id": "corp",
                "status": "FAILED",
                "error": "TimeoutError: timed out\nTraceback: hidden",
            }
        ],
    )

    assert "- corp: FAILED (TimeoutError: timed out Traceback: hidden)" in report
    assert "\nTraceback" not in report


def test_startup_report_handles_no_accounts() -> None:
    builder = LaunchReportBuilder(version_label="LetterBot.ru v26")
    report = builder.build(
        results=[], mode=SimpleNamespace(value="FULL"), mail_accounts=[]
    )

    assert "Mail accounts:" in report
    assert "- none configured" in report


def test_startup_report_degrades_if_mail_check_unavailable() -> None:
    builder = LaunchReportBuilder(version_label="LetterBot.ru v26")
    report = builder.build(
        results=[],
        mode=SimpleNamespace(value="FULL"),
        mail_check_unavailable_reason="RuntimeError: imap unavailable",
    )

    assert "Mail accounts:" in report
    assert "check unavailable (RuntimeError: imap unavailable)" in report


def test_dispatch_launch_report_does_not_raise(
    monkeypatch, caplog: pytest.LogCaptureFixture
) -> None:
    def raising_send(*_args, **_kwargs) -> bool:
        raise RuntimeError("send failed")

    monkeypatch.setattr(telegram_sender, "send_telegram", raising_send)
    with caplog.at_level(logging.ERROR):
        ok = dispatch_launch_report("token", "chat", "report")
    assert ok is False
    assert "launch_report_send_failed" in caplog.text


def test_startup_health_import_has_no_pipeline_side_effects(monkeypatch) -> None:
    # monkeypatch puts the modules back afterwards: a later lazy import must
    # not create second copies (tests patch the modules they imported).
    for module_name in list(sys.modules):
        if module_name.startswith("mailbot_v26.pipeline"):
            monkeypatch.delitem(sys.modules, module_name)
    importlib.reload(sys.modules["mailbot_v26.system.startup_health"])
    assert not any(name.startswith("mailbot_v26.pipeline") for name in sys.modules)


def test_evaluate_mode_updates_global_system_health(tmp_path, monkeypatch) -> None:
    _write_config_files(tmp_path, gigachat_enabled=False, gigachat_key="")
    config_dir = tmp_path / "config"
    config = load_config(config_dir)
    checker = StartupHealthChecker(config_dir, config)

    from mailbot_v26.system_health import OperationalMode, system_health

    system_health.reset()
    system_health.update_component("LLM", False)

    mode = checker.evaluate_mode(
        [
            {"component": "DB", "status": HealthStatus.OK, "details": "ok"},
            {"component": "Telegram", "status": HealthStatus.OK, "details": "ok"},
            {"component": "Cloudflare", "status": HealthStatus.OK, "details": "ok"},
            {
                "component": "GigaChat",
                "status": HealthStatus.DEGRADED,
                "details": "disabled",
            },
        ]
    )

    assert mode == OperationalMode.FULL
    assert system_health.mode == OperationalMode.FULL


def test_db_parent_dir_created_before_storage_init() -> None:
    source = Path("mailbot_v26/start.py").read_text(encoding="utf-8")

    mkdir_index = source.index(
        "_ensure_runtime_dirs(db_path=config.storage.db_path, log_path=LOG_PATH)"
    )
    storage_index = source.index("storage = Storage(config.storage.db_path)")

    assert mkdir_index < storage_index


def test_processor_and_start_use_same_db_path() -> None:
    source = Path("mailbot_v26/start.py").read_text(encoding="utf-8")

    assert (
        "processor_module.configure_processor_db_path(config.storage.db_path)" in source
    )
