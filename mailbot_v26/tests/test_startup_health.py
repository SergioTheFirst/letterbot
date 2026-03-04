import importlib
import logging
import sys
from types import SimpleNamespace

import pytest

from mailbot_v26.config_loader import load_config
from mailbot_v26.system.startup_health import (
    HealthStatus,
    LaunchReportBuilder,
    StartupHealthChecker,
    dispatch_launch_report,
)
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
    builder = LaunchReportBuilder(version_label="Letterbot Premium v26")
    results = [
        {"component": "Cloudflare", "status": HealthStatus.OK, "details": "active"},
        {"component": "GigaChat", "status": HealthStatus.DEGRADED, "details": "disabled"},
        {"component": "DB", "status": HealthStatus.OK, "details": "/db"},
        {"component": "OS", "status": HealthStatus.OK, "details": "Linux"},
        {"component": "Python", "status": HealthStatus.OK, "details": "3.11"},
        {"component": "Telegram", "status": HealthStatus.OK, "details": "reachable"},
    ]
    report_a = builder.build(results, mode=SimpleNamespace(value="FULL"))
    report_b = builder.build(list(reversed(results)), mode=SimpleNamespace(value="FULL"))
    assert report_a == report_b


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
                "details": "configured + direct test failed",
            }
        ],
        mode=SimpleNamespace(value="FULL"),
    )

    assert "LLM delivery mode: HEURISTIC_FALLBACK" in report
    assert "immediate TG summaries: heuristic" in report
    assert "background queue: disabled" in report
    assert "direct path check: configured + direct test failed" in report


def test_startup_report_default_branding_and_watermark() -> None:
    report = LaunchReportBuilder().build(results=[], mode=SimpleNamespace(value="FULL"))
    assert "Letterbot Premium" in report
    assert "MailBot Premium" not in report
    assert WATERMARK_LINE in report


def test_startup_report_includes_mail_account_status_ok() -> None:
    builder = LaunchReportBuilder(version_label="Letterbot Premium v26")
    report = builder.build(
        results=[],
        mode=SimpleNamespace(value="FULL"),
        mail_accounts=[{"account_id": "mos_ru", "status": "OK", "error": ""}],
    )

    assert "Mail accounts:" in report
    assert "- mos_ru: OK" in report


def test_startup_report_includes_mail_account_status_failed() -> None:
    builder = LaunchReportBuilder(version_label="Letterbot Premium v26")
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
    builder = LaunchReportBuilder(version_label="Letterbot Premium v26")
    report = builder.build(results=[], mode=SimpleNamespace(value="FULL"), mail_accounts=[])

    assert "Mail accounts:" in report
    assert "- none configured" in report


def test_startup_report_degrades_if_mail_check_unavailable() -> None:
    builder = LaunchReportBuilder(version_label="Letterbot Premium v26")
    report = builder.build(
        results=[],
        mode=SimpleNamespace(value="FULL"),
        mail_check_unavailable_reason="RuntimeError: imap unavailable",
    )

    assert "Mail accounts:" in report
    assert "check unavailable (RuntimeError: imap unavailable)" in report


def test_dispatch_launch_report_does_not_raise(monkeypatch, caplog: pytest.LogCaptureFixture) -> None:
    def raising_send(*_args, **_kwargs) -> bool:
        raise RuntimeError("send failed")

    monkeypatch.setattr(telegram_sender, "send_telegram", raising_send)
    with caplog.at_level(logging.ERROR):
        ok = dispatch_launch_report("token", "chat", "report")
    assert ok is False
    assert "launch_report_send_failed" in caplog.text


def test_startup_health_import_has_no_pipeline_side_effects(monkeypatch) -> None:
    for module_name in list(sys.modules):
        if module_name.startswith("mailbot_v26.pipeline"):
            del sys.modules[module_name]
    importlib.reload(sys.modules["mailbot_v26.system.startup_health"])
    assert not any(name.startswith("mailbot_v26.pipeline") for name in sys.modules)
