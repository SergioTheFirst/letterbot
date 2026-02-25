from __future__ import annotations

from mailbot_v26 import doctor


def test_print_lan_url_all_interfaces_uses_detected_ipv4(monkeypatch, tmp_path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=0.0.0.0\nport=8787\n", encoding="utf-8")
    monkeypatch.setattr(doctor, "get_primary_ipv4", lambda: "192.168.1.23")

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://192.168.1.23:8787/" in output


def test_print_lan_url_loopback_prints_local_only_hint(tmp_path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=127.0.0.1\nport=9000\n", encoding="utf-8")

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "Local only: http://127.0.0.1:9000/" in output
    assert "settings.ini" in output


def test_print_lan_url_explicit_ip_uses_bind_ip(tmp_path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=192.168.1.55\nport=8111\n", encoding="utf-8")

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://192.168.1.55:8111/" in output


def test_print_lan_url_when_ipv4_unknown_shows_ipconfig(monkeypatch, tmp_path, capsys) -> None:
    (tmp_path / "settings.ini").write_text("[web]\nhost=0.0.0.0\nport=8111\n", encoding="utf-8")
    monkeypatch.setattr(doctor, "get_primary_ipv4", lambda: None)

    code = doctor.print_lan_url(config_dir=tmp_path)

    output = capsys.readouterr().out
    assert code == 0
    assert "http://<PC IPv4>:8111/" in output
    assert "Run ipconfig" in output
