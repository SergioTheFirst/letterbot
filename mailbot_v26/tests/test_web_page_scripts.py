"""Page scripts live in static files and say when a refresh fails (P3b D2/A11).

Inline page scripts needed the CSP nonce and carried their configuration
as Jinja-interpolated JavaScript. They now load from /static/*.js, read
their settings from data-* attributes, use delegated listeners, and a
failed refresh keeps the last data on screen with a polite live-region
note ("Couldn't refresh; data from 14:05") instead of a bare "offline".
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import SupportMethod, SupportSettings, create_app

WEB_DIR = Path(web_app.__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
ACCOUNT = "primary@example.com"

PAGE_SCRIPTS = {
    "/": ["live.js", "cockpit.js"],
    "/health": ["live.js"],
    "/archive": ["archive.js"],
    "/relationships": ["relationships.js"],
    "/relationships/contact?contact_id=c1": ["relationships.js"],
    "/support": ["support.js"],
}
_SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)
_ID_CALL_RE = re.compile(
    r"(?:getElementById|byId|setText|setBadge|renderList|emptyText)\('([A-Za-z0-9_-]+)'"
)


class _AttrCollector(HTMLParser):
    def __init__(self, attribute: str) -> None:
        super().__init__()
        self.attribute = attribute
        self.found: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if self.attribute in values:
            self.found.append(values)

    handle_startendtag = handle_starttag


def _elements_with(body: str, attribute: str) -> list[dict[str, str | None]]:
    collector = _AttrCollector(attribute)
    collector.feed(body)
    return collector.found


def _app(tmp_path: Path):
    db_path = tmp_path / "scripts.sqlite"
    KnowledgeDB(db_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO emails (id, account_email, from_email, subject, received_at) "
            "VALUES (1, ?, 'a@example.com', 's', ?)",
            (ACCOUNT, now.isoformat()),
        )
        payload = json.dumps({"subtype": "success", "detail": "ok"})
        conn.execute(
            "INSERT INTO events_v1 (event_type, ts_utc, ts, account_id, entity_id, "
            "email_id, payload, payload_json, schema_version, fingerprint) "
            "VALUES ('imap_health_v1', ?, ?, ?, NULL, NULL, ?, ?, 1, 'imap-ok')",
            (now.timestamp(), now.isoformat(), ACCOUNT, payload, payload),
        )
        conn.commit()
    return create_app(
        db_path=db_path,
        password="pw",
        secret_key="secret",
        support_settings=SupportSettings(
            enabled=True,
            show_in_nav=True,
            methods=[
                SupportMethod("sbp", "SBP", "", "+70000000000", "2202", "", "", "")
            ],
        ),
    )


def test_templates_carry_no_inline_script_code() -> None:
    offenders: list[str] = []
    for template in sorted(TEMPLATES_DIR.rglob("*.html")):
        for attrs, content in _SCRIPT_RE.findall(template.read_text(encoding="utf-8")):
            if "src=" not in attrs or content.strip():
                offenders.append(f"{template.name}: inline script")
            if 'nonce="{{ csp_nonce }}"' not in attrs:
                offenders.append(f"{template.name}: script without nonce")
    assert offenders == []


@pytest.mark.parametrize(("path", "scripts"), sorted(PAGE_SCRIPTS.items()))
def test_pages_load_their_scripts_from_static(
    tmp_path: Path, path: str, scripts: list[str]
) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        body = client.get(path).get_data(as_text=True)
        sources = re.findall(r'<script\b[^>]*\bsrc="([^"]+)"', body)
        assert [src.rsplit("/", 1)[-1] for src in sources] == scripts
        for src in sources:
            assert src.startswith("/static/"), src
            response = client.get(src)
            assert response.status_code == 200, src
            assert "javascript" in (response.content_type or ""), src


def test_page_scripts_only_target_ids_their_pages_render(tmp_path: Path) -> None:
    app = _app(tmp_path)
    rendered: dict[str, str] = {}
    with app.test_client() as client:
        for path in PAGE_SCRIPTS:
            rendered[path] = client.get(path).get_data(as_text=True)

    for script in sorted({name for names in PAGE_SCRIPTS.values() for name in names}):
        pages = [rendered[path] for path, names in PAGE_SCRIPTS.items() if script in names]
        ids = set(_ID_CALL_RE.findall((STATIC_DIR / script).read_text(encoding="utf-8")))
        missing = sorted(i for i in ids if not any(f'id="{i}"' in page for page in pages))
        assert missing == [], script


@pytest.mark.parametrize("path", ["/", "/health"])
def test_live_pages_have_one_polite_refresh_status(tmp_path: Path, path: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        body = client.get(path).get_data(as_text=True)

    regions = _elements_with(body, "data-refresh-status")
    assert len(regions) == 1
    region = regions[0]
    assert region.get("role") == "status"
    assert region.get("aria-live") == "polite"
    assert region.get("data-error-text") == "Couldn't refresh; data from {time}"
    # The bare "offline" badges are gone; the live region replaces them.
    assert "status-strip-offline" not in body


def test_refresh_settings_come_from_data_attributes(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        home = client.get("/").get_data(as_text=True)
        health = client.get("/health").get_data(as_text=True)

    dashboard = _elements_with(home, "data-dashboard-url")
    assert len(dashboard) == 1
    assert dashboard[0]["data-dashboard-url"] == "/api/dashboard"
    assert dashboard[0]["data-dashboard-refresh-ms"] == str(web_app.DASHBOARD_REFRESH_MS)

    def partials(body: str) -> dict[str | None, str | None]:
        return {
            item["data-live-partial"]: item.get("data-live-interval-ms")
            for item in _elements_with(body, "data-live-partial")
        }

    strip_ms = str(web_app.STATUS_STRIP_REFRESH_MS)
    assert partials(home) == {"/partial/status_strip": strip_ms}
    assert partials(health) == {
        "/partial/status_strip": strip_ms,
        "/partial/health": str(web_app.HEALTH_REFRESH_MS),
    }


def test_live_refresh_pauses_while_hidden_and_reports_failures() -> None:
    source = (STATIC_DIR / "live.js").read_text(encoding="utf-8")
    intervals = re.findall(r"setInterval\(function \(\) \{([^}]*)\}", source)
    assert intervals
    for body in intervals:
        assert "document.hidden" in body
    assert "visibilitychange" in source
    assert "[data-refresh-status]" in source
    assert "{time}" in source
    # Page scripts reuse the shared loop instead of their own timers.
    cockpit = (STATIC_DIR / "cockpit.js").read_text(encoding="utf-8")
    assert "setInterval" not in cockpit
    assert "LetterbotLive.register(" in cockpit
    assert "LetterbotLive.report(" in cockpit


@pytest.mark.parametrize("script", ["archive.js", "support.js"])
def test_click_handlers_are_delegated(script: str) -> None:
    source = (STATIC_DIR / script).read_text(encoding="utf-8")
    assert "document.addEventListener('click'" in source
    assert "onclick" not in source
