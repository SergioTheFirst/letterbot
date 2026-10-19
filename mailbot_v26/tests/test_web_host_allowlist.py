"""QW19 (finding C3, DNS rebinding): the web UI only answers known Host names."""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import create_app

LAN_IP = "192.168.1.50"
LAN_ALIAS_IP = "192.168.1.51"
LAN_CLIENT = {"REMOTE_ADDR": "192.168.1.20"}


def _app(tmp_path: Path, **kwargs):
    db_path = tmp_path / "host.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret", **kwargs)


@pytest.fixture
def fake_lan(monkeypatch):
    monkeypatch.setattr(web_app, "get_primary_ipv4", lambda: LAN_IP)
    monkeypatch.setattr(web_app.socket, "gethostname", lambda: "TESTPC")
    monkeypatch.setattr(
        web_app.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (LAN_ALIAS_IP, 0))
        ],
    )


@pytest.mark.parametrize(
    "host",
    ["localhost", "localhost:8080", "LOCALHOST:8080", "127.0.0.1:8080", "[::1]:8080"],
)
def test_loopback_hosts_are_accepted(tmp_path: Path, host: str) -> None:
    app = _app(tmp_path, web_ui_port=8080)
    with app.test_client() as client:
        response = client.get("/doctor", headers={"Host": host})
    assert response.status_code == 200


def test_default_test_client_host_still_works(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        assert client.get("/doctor").status_code == 200


@pytest.mark.parametrize(
    "host",
    [
        "evil.example",
        "evil.example:8080",
        "localhost.evil.example:8080",
        "127.0.0.1.nip.io:8080",
        "localhost:9999",
        f"{LAN_IP}:8080",
    ],
)
def test_unknown_hosts_are_rejected(tmp_path: Path, host: str) -> None:
    app = _app(tmp_path, web_ui_port=8080)
    with app.test_client() as client:
        page = client.get("/doctor", headers={"Host": host})
        static = client.get("/static/style.css", headers={"Host": host})
    assert page.status_code == 400
    assert static.status_code == 400


def test_smoke_bypass_does_not_skip_host_check(tmp_path: Path) -> None:
    app = _app(tmp_path, web_ui_port=8080, allow_local_smoke_bypass=True)
    with app.test_client() as client:
        response = client.get("/doctor", headers={"Host": "evil.example:8080"})
    assert response.status_code == 400


def test_specific_bind_address_is_accepted(tmp_path: Path) -> None:
    app = _app(tmp_path, web_ui_bind="192.168.1.77", web_ui_port=8787)
    with app.test_client() as client:
        response = client.get("/doctor", headers={"Host": "192.168.1.77:8787"})
    assert response.status_code == 200


def test_lan_mode_accepts_machine_addresses(tmp_path: Path, fake_lan) -> None:
    app = _app(
        tmp_path,
        web_ui_bind="0.0.0.0",
        web_ui_port=8787,
        allow_cidrs=["192.168.0.0/16"],
    )
    with app.test_client() as client:
        for host in (f"{LAN_IP}:8787", f"{LAN_ALIAS_IP}:8787", "testpc:8787"):
            response = client.get(
                "/doctor", headers={"Host": host}, environ_base=LAN_CLIENT
            )
            assert response.status_code == 200, host
        rejected = client.get(
            "/doctor", headers={"Host": "evil.example:8787"}, environ_base=LAN_CLIENT
        )
    assert rejected.status_code == 400
