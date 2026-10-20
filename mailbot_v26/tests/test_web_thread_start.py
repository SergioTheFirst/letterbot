"""C15: the frozen in-process web UI reports readiness honestly.

start_web_in_thread returns only after the server listens and raises when it
cannot start (busy port, werkzeug's SystemExit, any OSError), so the caller's
"[OK] Web UI ..." line is printed only for a working server. The hard-coded
local smoke bypass (which skipped CSRF) is gone.
"""

from __future__ import annotations

import errno
import http.client
import logging
import socket
import threading
from pathlib import Path

import pytest
import werkzeug.serving

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app


def _db(tmp_path: Path) -> Path:
    db_path = tmp_path / "web.sqlite"
    KnowledgeDB(db_path)
    return db_path


def _busy_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
        # Windows: keep the second bind from sharing the port.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    return sock


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _FakeServer:
    def __init__(self) -> None:
        self.served = threading.Event()

    def serve_forever(self) -> None:
        self.served.set()

    def shutdown(self) -> None:
        return None

    def server_close(self) -> None:
        return None


def test_busy_port_raises_and_prints_clear_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    busy = _busy_socket()
    port = int(busy.getsockname()[1])
    try:
        with pytest.raises(RuntimeError) as excinfo:
            web_app.start_web_in_thread(
                config_dir=tmp_path,
                db_path=_db(tmp_path),
                host="127.0.0.1",
                port=port,
                ready_timeout=15,
            )
    finally:
        busy.close()
    out = capsys.readouterr().out
    assert "[ERROR]" in out
    assert f"127.0.0.1:{port}" in out
    assert "settings.ini" in out
    assert f"127.0.0.1:{port}" in str(excinfo.value)
    assert "web_thread_failed" in caplog.text


@pytest.mark.parametrize(
    "failure",
    [SystemExit(1), OSError(errno.EADDRNOTAVAIL, "address not available")],
)
def test_bind_failure_is_reported_not_swallowed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: BaseException,
) -> None:
    def _raise(*args, **kwargs):
        raise failure

    monkeypatch.setattr(werkzeug.serving, "make_server", _raise)
    with pytest.raises(RuntimeError):
        web_app.start_web_in_thread(
            config_dir=tmp_path,
            db_path=_db(tmp_path),
            host="127.0.0.1",
            port=8787,
            ready_timeout=15,
        )
    assert "[ERROR] Web UI could not start on 127.0.0.1:8787" in capsys.readouterr().out


def test_not_listening_within_timeout_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = threading.Event()

    def _hang(*args, **kwargs):
        release.wait(30)
        return _FakeServer()

    monkeypatch.setattr(werkzeug.serving, "make_server", _hang)
    try:
        with pytest.raises(RuntimeError, match="not listening"):
            web_app.start_web_in_thread(
                config_dir=tmp_path,
                db_path=_db(tmp_path),
                host="127.0.0.1",
                port=8787,
                ready_timeout=0.2,
            )
    finally:
        release.set()


def test_returns_only_once_server_listens(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    port = _free_port()
    server = web_app.start_web_in_thread(
        config_dir=tmp_path,
        db_path=_db(tmp_path),
        host="127.0.0.1",
        port=port,
        ready_timeout=15,
    )
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
        conn.request("GET", "/doctor")
        response = conn.getresponse()
        response.read()
        conn.close()
        assert response.status == 200
    finally:
        server.shutdown()
        server.server_close()
    assert "WEB_UI_LISTENING" in caplog.text


def _capture_create_app(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    captured: dict[str, object] = {}
    real_create_app = web_app.create_app

    def _spy(**kwargs):
        captured.update(kwargs)
        return real_create_app(**kwargs)

    monkeypatch.setattr(web_app, "create_app", _spy)
    monkeypatch.setattr(
        werkzeug.serving, "make_server", lambda *args, **kwargs: _FakeServer()
    )
    return captured


def test_frozen_web_thread_keeps_csrf_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = _capture_create_app(monkeypatch)
    web_app.start_web_in_thread(
        config_dir=tmp_path, db_path=_db(tmp_path), port=8787, ready_timeout=15
    )
    assert captured["allow_local_smoke_bypass"] is False


def test_frozen_web_thread_honours_configured_smoke_bypass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    (tmp_path / "settings.ini").write_text(
        "[web_ui]\nallow_local_smoke_bypass = true\n", encoding="utf-8"
    )
    captured = _capture_create_app(monkeypatch)
    web_app.start_web_in_thread(
        config_dir=tmp_path, db_path=_db(tmp_path), port=8787, ready_timeout=15
    )
    assert captured["allow_local_smoke_bypass"] is True
    assert "WEB_UI_LOCAL_SMOKE_BYPASS_ACTIVE" in caplog.text
