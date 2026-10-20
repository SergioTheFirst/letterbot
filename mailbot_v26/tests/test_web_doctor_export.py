from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from mailbot_v26.integrity import compute_manifest
from mailbot_v26.tests._web_helpers import login_with_csrf, post_doctor_export_with_csrf
from mailbot_v26.web_observability import doctor_export
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.web_observability.doctor_export import build_diagnostics_zip

_FAKE_BOT_TOKEN = "123456789:" + "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
_SENSITIVE_LOG = (
    "2026-10-01 ERROR telegram send failed: "
    f"https://api.telegram.org/bot{_FAKE_BOT_TOKEN}/sendMessage\n"
    f"2026-10-01 ERROR bare token leaked {_FAKE_BOT_TOKEN}\n"
    "2026-10-01 WARNING LLM stage is not configured; "
    "subject=Invoice 42 for ACME from=ivan.petrov@example.com attachments=2\n"
    "2026-10-01 INFO reply from jane.doe@corp.example.org recorded\n"
)


def _read_zip(payload: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _assert_log_scrubbed(text: str) -> None:
    assert _FAKE_BOT_TOKEN not in text
    assert _FAKE_BOT_TOKEN.split(":", 1)[1] not in text
    assert "ivan.petrov@example.com" not in text
    assert "jane.doe@corp.example.org" not in text
    assert "Invoice 42 for ACME" not in text
    assert "j…@corp.example.org" in text
    assert "attachments=2" in text


@pytest.mark.parametrize("truncate", [False, True])
def test_doctor_zip_scrubs_log_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, truncate: bool
) -> None:
    if truncate:
        monkeypatch.setattr(doctor_export, "MAX_LOG_BYTES", 16)
    config_path = tmp_path / "config.yaml"
    config_path.write_text("app_name: test\n", encoding="utf-8")
    log_path = tmp_path / "mailbot.log"
    log_path.write_text(_SENSITIVE_LOG, encoding="utf-8")
    log_path.with_name("mailbot.log.1").write_text(_SENSITIVE_LOG, encoding="utf-8")

    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=tmp_path / "mailbot.sqlite",
        dist_root=tmp_path,
    )

    files = _read_zip(payload)
    for name in ("logs/mailbot.log", "logs/mailbot.log.1"):
        _assert_log_scrubbed(files[name].decode("utf-8"))


def test_doctor_zip_scrubs_config_and_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C9: account logins (e-mail addresses) and subjects never leave raw."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "accounts:\n"
        "  - login: ivan.petrov@example.com\n"
        "    password: hunter2\n"
        "note: subject=Invoice 42 for ACME\n",
        encoding="utf-8",
    )
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("ok\n", encoding="utf-8")
    monkeypatch.setattr(
        doctor_export,
        "_latest_health_snapshot",
        lambda _db_path: {
            "accounts": {
                "jane.doe@corp.example.org": {"status": "ok"},
                "jack.doe@corp.example.org": {"status": "warn"},
            },
            "last_error": f"login failed for ivan.petrov@example.com bot{_FAKE_BOT_TOKEN}",
            "recent": ["subject=Invoice 42 for ACME from=ivan.petrov@example.com"],
            "count": 3,
        },
    )

    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=tmp_path / "mailbot.sqlite",
        dist_root=tmp_path,
    )

    files = _read_zip(payload)
    config_text = files["config/config.redacted.yaml"].decode("utf-8")
    assert "ivan.petrov@example.com" not in config_text
    assert "login: i…@example.com" in config_text
    assert "hunter2" not in config_text
    assert "Invoice 42 for ACME" not in config_text

    health_text = files["health/health.json"].decode("utf-8")
    health = json.loads(health_text)
    for raw in (
        "jane.doe@corp.example.org",
        "jack.doe@corp.example.org",
        "ivan.petrov@example.com",
        "Invoice 42 for ACME",
        _FAKE_BOT_TOKEN.split(":", 1)[1],
    ):
        assert raw not in health_text
    # Masked keys that collide stay distinct instead of overwriting each other.
    assert sorted(health["accounts"]) == ["j…@corp.example.org", "j…@corp.example.org#2"]
    assert health["count"] == 3
    assert health["recent"] == ["subject=<redacted> from=<redacted>"]


def test_doctor_redacts_secrets(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
password: my-password
api_token: very-secret-token
telegram_token: tg-secret
regular_field: keep-me
""".strip() + "\n",
        encoding="utf-8",
    )
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("ok\n", encoding="utf-8")
    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=tmp_path / "mailbot.sqlite",
        dist_root=tmp_path,
    )
    files = _read_zip(payload)
    redacted = files["config/config.redacted.yaml"].decode("utf-8")
    assert "my-password" not in redacted
    assert "very-secret-token" not in redacted
    assert "tg-secret" not in redacted
    assert "***REDACTED***" in redacted
    assert "regular_field: keep-me" in redacted


def test_doctor_zip_contains_expected_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("app_name: test\n", encoding="utf-8")
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("line\n", encoding="utf-8")
    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=tmp_path / "mailbot.sqlite",
        dist_root=tmp_path,
    )
    files = _read_zip(payload)
    assert "logs/mailbot.log" in files
    assert "health/health.json" in files
    assert "config/config.redacted.yaml" in files
    assert "versions/runtime.txt" in files


def test_doctor_manifest_status_included_when_present(tmp_path: Path) -> None:
    target = tmp_path / "payload.txt"
    target.write_text("stable", encoding="utf-8")
    manifest = compute_manifest(tmp_path)
    (tmp_path / "manifest.sha256.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: y\n", encoding="utf-8")
    log_path = tmp_path / "mailbot.log"
    log_path.write_text("line\n", encoding="utf-8")
    payload = build_diagnostics_zip(
        config_path=config_path,
        log_path=log_path,
        db_path=tmp_path / "mailbot.sqlite",
        dist_root=tmp_path,
    )
    files = _read_zip(payload)
    manifest_payload = json.loads(files["build/manifest_status.json"].decode("utf-8"))
    assert manifest_payload["status"] in {"OK", "MODIFIED"}
    assert "changed_files" in manifest_payload


def test_doctor_requires_auth(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        response = client.post("/doctor/export")
    assert response.status_code in {302, 403}
    if response.status_code == 302:
        assert "/login" in response.headers.get("Location", "")


def test_doctor_export_csrf_blocks_without_token(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.post("/doctor/export")

    assert response.status_code == 403


def test_doctor_export_csrf_allows_with_valid_token(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = post_doctor_export_with_csrf(client)

    assert response.status_code == 200
    assert response.headers.get("Content-Type") == "application/zip"


def test_doctor_page_renders_export_cta(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/doctor")

    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Diagnostics export" in body
    assert "Export diagnostics.zip" in body
    assert "Log path" in body


def test_doctor_page_safe_state_without_manifest(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        response = client.get("/doctor")

    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "NO_MANIFEST" in body
    assert "Integrity manifest is unavailable." in body
