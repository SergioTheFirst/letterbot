from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

from mailbot_v26.integrity import compute_manifest
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.web_observability.doctor_export import build_diagnostics_zip




def _extract_csrf_token(html_text: str) -> str:
    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html_text)
    assert match is not None
    return match.group(1)


def _login_with_csrf(client, password: str) -> None:
    login_page = client.get("/login")
    token = _extract_csrf_token(login_page.get_data(as_text=True))
    response = client.post("/login", data={"password": password, "csrf_token": token})
    assert response.status_code in {302, 303}

def _read_zip(payload: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def test_doctor_redacts_secrets(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
password: my-password
api_token: very-secret-token
telegram_token: tg-secret
regular_field: keep-me
""".strip()
        + "\n",
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
        _login_with_csrf(client, "pw")
        response = client.post("/doctor/export")

    assert response.status_code == 403


def test_doctor_export_csrf_allows_with_valid_token(tmp_path: Path) -> None:
    db_path = tmp_path / "mailbot.sqlite"
    db_path.write_bytes(b"")
    app = create_app(db_path=db_path, password="pw", secret_key="secret")
    with app.test_client() as client:
        _login_with_csrf(client, "pw")
        doctor_page = client.get("/doctor")
        doctor_token = _extract_csrf_token(doctor_page.get_data(as_text=True))
        response = client.post("/doctor/export", data={"csrf_token": doctor_token})

    assert response.status_code == 200
    assert response.headers.get("Content-Type") == "application/zip"
