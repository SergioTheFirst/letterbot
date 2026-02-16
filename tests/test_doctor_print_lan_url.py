from __future__ import annotations

from pathlib import Path

from mailbot_v26 import doctor


def _base_config(*, bind: str, port: int, enabled: bool = True) -> dict[str, object]:
    return {
        "telegram": {"bot_token": "token", "chat_id": "123"},
        "llm": {
            "provider": "cloudflare",
            "cloudflare": {"api_token": "cf-token", "account_id": "cf-account"},
            "gigachat": {"api_token": ""},
        },
        "accounts": [
            {
                "name": "primary",
                "email": "user@example.com",
                "imap_host": "imap.example.com",
                "imap_port": 993,
                "username": "user@example.com",
                "password": "secret",
                "enabled": True,
            }
        ],
        "polling": {"interval_seconds": 60, "reload_config_seconds": 60},
        "web_ui": {
            "enabled": enabled,
            "bind": bind,
            "port": port,
            "password": "1234567890",
            "allow_lan": bind != "127.0.0.1",
            "allow_cidrs": ["192.168.0.0/16"] if bind != "127.0.0.1" else [],
            "prod_server": bind != "127.0.0.1",
        },
    }


def _stub_config(monkeypatch, tmp_path: Path, cfg: dict[str, object]) -> None:
    (tmp_path / "config.yaml").write_text("stub", encoding="utf-8")
    monkeypatch.setattr(doctor, "load_yaml_config", lambda _path: cfg)
    monkeypatch.setattr(doctor, "validate_yaml_config", lambda _raw: (True, None))


def test_print_lan_url_all_interfaces_uses_detected_ipv4(monkeypatch, tmp_path, capsys) -> None:
    _stub_config(monkeypatch, tmp_path, _base_config(bind="0.0.0.0", port=8787))
    monkeypatch.setattr(doctor, "get_primary_ipv4", lambda: "192.168.1.23")

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://192.168.1.23:8787/" in output
    assert "0.0.0.0" not in output


def test_print_lan_url_loopback_prints_local_only_hint(monkeypatch, tmp_path, capsys) -> None:
    _stub_config(monkeypatch, tmp_path, _base_config(bind="127.0.0.1", port=9000))

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "Local only: http://127.0.0.1:9000/" in output
    assert "Not reachable from LAN" in output


def test_print_lan_url_explicit_ip_uses_bind_ip(monkeypatch, tmp_path, capsys) -> None:
    _stub_config(monkeypatch, tmp_path, _base_config(bind="192.168.1.55", port=8111))

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://192.168.1.55:8111/" in output


def test_print_lan_url_when_ipv4_unknown_shows_ipconfig(monkeypatch, tmp_path, capsys) -> None:
    _stub_config(monkeypatch, tmp_path, _base_config(bind="0.0.0.0", port=8111))
    monkeypatch.setattr(doctor, "get_primary_ipv4", lambda: None)

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://<PC IPv4>:8111/" in output
    assert "Run ipconfig" in output


def test_print_lan_url_web_ui_disabled_returns_code_2(monkeypatch, tmp_path, capsys) -> None:
    _stub_config(monkeypatch, tmp_path, _base_config(bind="127.0.0.1", port=8111, enabled=False))

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 2
    assert "Web UI disabled in config" in output
