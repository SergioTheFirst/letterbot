"""WP3b: nonce-based CSP and hardening headers on every web response.

Inline scripts run only with the per-request nonce, and templates carry no
inline event-handler attributes (CSP would block them silently).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import SupportMethod, SupportSettings, create_app

TEMPLATES_DIR = Path(web_app.__file__).resolve().parent / "templates"
_CSP_NONCE_RE = re.compile(r"script-src 'self' 'nonce-([A-Za-z0-9_\-]+)'")
_SCRIPT_TAG_RE = re.compile(r"<script\b[^>]*>", re.IGNORECASE)
_INLINE_HANDLER_RE = re.compile(r"\son[a-z]+\s*=", re.IGNORECASE)


def _app(tmp_path: Path):
    db_path = tmp_path / "headers.sqlite"
    KnowledgeDB(db_path)
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


def _csp_nonce(response) -> str:
    csp = response.headers.get("Content-Security-Policy", "")
    match = _CSP_NONCE_RE.search(csp)
    assert match, csp
    return match.group(1)


def _assert_hardening_headers(response) -> None:
    csp = response.headers.get("Content-Security-Policy", "")
    assert "object-src 'none'" in csp
    assert "base-uri 'none'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "unsafe-inline" not in csp
    assert response.headers.get("X-Frame-Options") == "DENY"
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("Referrer-Policy") == "no-referrer"


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/archive",
        "/health",
        "/relationships",
        "/relationships/contact?contact_id=c1",
        "/support",
    ],
)
def test_inline_scripts_carry_the_response_nonce(tmp_path: Path, path: str) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(path)
    assert response.status_code == 200
    _assert_hardening_headers(response)
    nonce = _csp_nonce(response)
    tags = _SCRIPT_TAG_RE.findall(response.get_data(as_text=True))
    assert tags, f"expected an inline script on {path}"
    for tag in tags:
        assert f'nonce="{nonce}"' in tag, tag


def test_nonce_changes_per_request(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        first = _csp_nonce(client.get("/health"))
        second = _csp_nonce(client.get("/health"))
    assert first != second
    assert len(first) >= 16


@pytest.mark.parametrize(
    ("path", "headers", "environ", "status"),
    [
        ("/static/style.css", {}, {}, 200),
        ("/doctor", {"Host": "evil.example"}, {}, 400),
        ("/doctor", {}, {"REMOTE_ADDR": "203.0.113.7"}, 403),
        ("/login", {}, {}, 302),
        ("/no-such-page", {}, {}, 404),
    ],
)
def test_headers_on_every_response_kind(
    tmp_path: Path, path: str, headers: dict, environ: dict, status: int
) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(path, headers=headers, environ_base=environ)
    assert response.status_code == status
    _assert_hardening_headers(response)


def test_templates_have_no_inline_event_handlers() -> None:
    offenders = []
    for template in sorted(TEMPLATES_DIR.rglob("*.html")):
        for lineno, line in enumerate(
            template.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if _INLINE_HANDLER_RE.search(line):
                offenders.append(f"{template.name}:{lineno}: {line.strip()}")
    assert offenders == []


def test_every_template_script_tag_uses_the_nonce() -> None:
    missing = []
    for template in sorted(TEMPLATES_DIR.rglob("*.html")):
        for tag in _SCRIPT_TAG_RE.findall(template.read_text(encoding="utf-8")):
            if 'nonce="{{ csp_nonce }}"' not in tag:
                missing.append(f"{template.name}: {tag}")
    assert missing == []


def test_support_copy_buttons_are_wired_without_inline_handlers(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        body = client.get("/support").get_data(as_text=True)
    assert 'data-copy-target="phone-1"' in body
    assert 'data-copy-target="card-1"' in body
    assert "addEventListener" in body
    assert "onclick" not in body
