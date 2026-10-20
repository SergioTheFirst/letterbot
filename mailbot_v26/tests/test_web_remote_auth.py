"""C3: every non-loopback request must authenticate; loopback stays login-free.

Remote clients (inside web_ui.allow_cidrs) sign in with the web password or
send the API token in the X-Api-Token header. Without any configured
credential remote access fails closed with a 403 page that says how to fix it.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import unquote

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests._web_helpers import extract_csrf_token
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import create_app

LAN = {"REMOTE_ADDR": "192.168.1.20"}
OTHER_LAN = {"REMOTE_ADDR": "192.168.1.21"}
PASSWORD = "correct horse battery"
TOKEN = "lan-api-token"


@pytest.fixture(autouse=True)
def _no_env_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WEB_OBSERVABILITY_TOKEN", raising=False)


def _app(tmp_path: Path, *, password: str = PASSWORD, api_token: str = TOKEN, **kwargs):
    db_path = tmp_path / "remote.sqlite"
    KnowledgeDB(db_path)
    return create_app(
        db_path=db_path,
        password=password,
        secret_key="secret",
        api_token=api_token,
        allow_cidrs=["192.168.0.0/16"],
        **kwargs,
    )


def _login(client, password: str, *, environ=LAN, next_path: str = ""):
    page = client.get("/login", environ_base=environ)
    assert page.status_code == 200
    token = extract_csrf_token(page.get_data(as_text=True))
    data = {"password": password, "csrf_token": token}
    if next_path:
        data["next"] = next_path
    return client.post("/login", data=data, environ_base=environ)


def test_loopback_stays_login_free(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        assert client.get("/doctor").status_code == 200
        assert client.get("/api/health/status").status_code == 200


def test_lan_page_without_auth_redirects_to_login(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get("/doctor?x=1", environ_base=LAN)
    assert response.status_code == 302
    location = response.headers["Location"]
    assert location.startswith("/login?next=")
    assert unquote(location.split("next=", 1)[1]) == "/doctor?x=1"


@pytest.mark.parametrize("path", ["/api/health/status", "/partial/status_strip"])
def test_lan_api_and_partials_without_auth_get_401(tmp_path: Path, path: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(path, environ_base=LAN)
    assert response.status_code == 401


def test_lan_post_without_auth_gets_401(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.post("/doctor/export", environ_base=LAN)
    assert response.status_code == 401


def test_lan_static_assets_stay_open(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        assert client.get("/static/style.css", environ_base=LAN).status_code == 200


def test_lan_login_page_renders_password_form(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get("/login?next=/doctor", environ_base=LAN)
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert 'name="password"' in body
    assert 'type="password"' in body
    assert extract_csrf_token(body)
    assert 'value="/doctor"' in body


def test_lan_login_with_password_grants_session(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    with app.test_client() as client:
        response = _login(client, PASSWORD, next_path="/doctor")
        assert response.status_code == 302
        assert response.headers["Location"] == "/doctor"
        assert client.get("/doctor", environ_base=LAN).status_code == 200
        assert client.get("/api/health/status", environ_base=LAN).status_code == 200
        # An authenticated LAN visit to /login goes straight on.
        again = client.get("/login?next=/archive", environ_base=LAN)
        assert again.status_code == 302
        assert again.headers["Location"] == "/archive"
    assert "WEB_UI_LOGIN_OK" in caplog.text
    assert PASSWORD not in caplog.text


def test_lan_login_rejects_off_site_next(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = _login(client, PASSWORD, next_path="/\\evil.example")
    assert response.status_code == 302
    assert response.headers["Location"] == "/"


def test_lan_wrong_password_is_rejected(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    with app.test_client() as client:
        response = _login(client, "wrong-password")
        assert response.status_code == 401
        assert 'name="password"' in response.get_data(as_text=True)
        assert client.get("/doctor", environ_base=LAN).status_code == 302
    assert "WEB_UI_LOGIN_FAILED" in caplog.text
    assert "wrong-password" not in caplog.text


def test_lan_login_requires_csrf_token(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        client.get("/login", environ_base=LAN)
        response = client.post("/login", data={"password": PASSWORD}, environ_base=LAN)
        assert response.status_code == 403
        assert client.get("/doctor", environ_base=LAN).status_code == 302


def test_lan_api_token_header_authenticates(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        ok = client.get(
            "/api/health/status", environ_base=LAN, headers={"X-Api-Token": TOKEN}
        )
        wrong = client.get(
            "/api/health/status", environ_base=LAN, headers={"X-Api-Token": "nope"}
        )
        non_ascii = client.get(
            "/api/health/status", environ_base=LAN, headers={"X-Api-Token": "tök"}
        )
        query = client.get(f"/api/health/status?token={TOKEN}", environ_base=LAN)
    assert ok.status_code == 200
    assert wrong.status_code == 401
    assert non_ascii.status_code == 401
    # A token in the URL would end up in logs and referrers: not accepted here.
    assert query.status_code == 401


def test_lan_login_accepts_api_token_when_no_password(tmp_path: Path) -> None:
    app = _app(tmp_path, password="")
    with app.test_client() as client:
        response = _login(client, TOKEN)
        assert response.status_code == 302
        assert client.get("/doctor", environ_base=LAN).status_code == 200


def test_lan_fails_closed_without_credentials(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = _app(tmp_path, password="", api_token="")
    assert "WEB_UI_REMOTE_AUTH_NOT_CONFIGURED" in caplog.text
    with app.test_client() as client:
        page = client.get("/doctor", environ_base=LAN)
        login = client.get("/login", environ_base=LAN)
        api = client.get("/api/health/status", environ_base=LAN)
        local = client.get("/doctor")
    assert page.status_code == 403
    assert login.status_code == 403
    assert api.status_code == 403
    body = page.get_data(as_text=True)
    assert "password" in body
    assert "api_token" in body
    assert "settings.ini" in body
    assert local.status_code == 200


def test_forwarded_for_does_not_bypass_remote_auth(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(
            "/doctor", environ_base=LAN, headers={"X-Forwarded-For": "127.0.0.1"}
        )
    assert response.status_code == 302


def test_lan_session_expires(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        assert _login(client, PASSWORD).status_code == 302
        with client.session_transaction() as sess:
            sess["authenticated_at"] = time.time() - (
                web_app.REMOTE_SESSION_MAX_AGE_SECONDS + 60
            )
        assert client.get("/doctor", environ_base=LAN).status_code == 302


def test_forged_authenticated_flag_without_timestamp_is_rejected(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["authenticated"] = True
        assert client.get("/doctor", environ_base=LAN).status_code == 302


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_lan_login_backoff_per_client(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    app = _app(tmp_path)
    clock = _FakeClock()
    app.config["WEB_UI_LOGIN_THROTTLE"] = web_app._LoginThrottle(clock=clock)
    free = web_app._LoginThrottle.FREE_ATTEMPTS
    with app.test_client() as client:
        for _ in range(free + 1):
            assert _login(client, "bad").status_code == 401
        locked = _login(client, PASSWORD)
        assert locked.status_code == 429
        assert int(locked.headers["Retry-After"]) >= 1
        assert client.get("/doctor", environ_base=LAN).status_code == 302
    with app.test_client() as other:
        # Another LAN client is not punished for the first one's failures.
        assert _login(other, PASSWORD, environ=OTHER_LAN).status_code == 302
    clock.now += web_app._LoginThrottle.MAX_DELAY_SECONDS + 1
    with app.test_client() as client:
        assert _login(client, PASSWORD).status_code == 302
        assert client.get("/doctor", environ_base=LAN).status_code == 200
    assert "WEB_UI_LOGIN_THROTTLED" in caplog.text


def test_login_throttle_backoff_grows_and_is_capped() -> None:
    clock = _FakeClock()
    throttle = web_app._LoginThrottle(clock=clock)
    delays = [throttle.record_failure("ip")[1] for _ in range(40)]
    free = web_app._LoginThrottle.FREE_ATTEMPTS
    assert delays[:free] == [0] * free
    assert delays[free] >= 1
    assert delays[free + 1] > delays[free]
    assert max(delays) == int(web_app._LoginThrottle.MAX_DELAY_SECONDS)
    throttle.reset("ip")
    assert throttle.retry_after("ip") == 0


def test_login_throttle_tracks_bounded_number_of_clients() -> None:
    throttle = web_app._LoginThrottle(clock=_FakeClock())
    for index in range(web_app._LoginThrottle.MAX_TRACKED_CLIENTS + 50):
        throttle.record_failure(f"10.0.{index // 256}.{index % 256}")
    assert len(throttle._state) <= web_app._LoginThrottle.MAX_TRACKED_CLIENTS


def test_loopback_login_route_still_redirects(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        get = client.get("/login?next=/doctor")
        post = client.post("/login?next=/doctor", data={"password": "x"})
    assert get.status_code == 302
    assert get.headers["Location"] == "/doctor"
    assert post.status_code == 302
