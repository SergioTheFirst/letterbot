from pathlib import Path
import re

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf


def _build_nav_app(tmp_path: Path):
    db_path = tmp_path / "nav.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_nav_contains_all_required_links(tmp_path: Path) -> None:
    """All nav links must be present in the rendered cockpit page."""
    app = _build_nav_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert resp.status_code == 200
        required_hrefs = [
            "/accounts",
            "/archive",
            "/health",
            "/events",
            "/doctor",
            "/commitments",
            "/latency",
            "/attention",
            "/learning",
            "/relationships",
        ]
        for href in required_hrefs:
            assert f'href="{href}"' in body or href in body, (
                f"Nav link {href} is missing from the cockpit page"
            )


def test_every_dashboard_page_links_to_accounts(tmp_path: Path) -> None:
    """The Accounts/Settings page is reachable from every HTML page, 404 included."""
    db_path = tmp_path / "nav.sqlite"
    KnowledgeDB(db_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    app = create_app(
        db_path=db_path, password="pw", secret_key="secret", config_dir=config_dir
    )
    paths = sorted(
        rule.rule
        for rule in app.url_map.iter_rules()
        if "GET" in (rule.methods or set())
        and not rule.arguments
        and rule.endpoint not in {"static", "login"}
        and not rule.rule.startswith(("/api/", "/partial/"))
    )
    checked: list[str] = []
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        for path in [*paths, "/no-such-page"]:
            resp = client.get(path)
            if resp.status_code not in (200, 404) or resp.mimetype != "text/html":
                continue
            body = resp.get_data(as_text=True)
            assert 'href="/accounts"' in body, f"{path} has no link to /accounts"
            checked.append(path)
    for path in ("/", "/archive", "/health", "/doctor", "/accounts", "/no-such-page"):
        assert path in checked, f"{path} was not rendered as an HTML page"


def test_learning_page_uses_dark_theme(tmp_path: Path) -> None:
    """learning page must not contain old standalone navbar class."""
    app = _build_nav_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/learning")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "LetterBot.ru" in body
        assert '<header class="navbar">' not in body


def test_relationships_page_uses_dark_theme(tmp_path: Path) -> None:
    """relationships page must not contain old standalone navbar class."""
    app = _build_nav_app(tmp_path)
    with app.test_client() as client:
        login_with_csrf(client, "pw")
        resp = client.get("/relationships")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "LetterBot.ru" in body
        assert '<header class="navbar">' not in body


def test_topbar_has_no_white_background(tmp_path: Path) -> None:
    """style.css must not have white rgba(255,255,255) topbar background."""
    _ = tmp_path
    css_path = Path("mailbot_v26/web_observability/static/style.css")
    if not css_path.exists():
        import pytest

        pytest.skip("style.css not found")
    css = css_path.read_text(encoding="utf-8")
    topbar_match = re.search(r"\.topbar\s*\{([^}]+)\}", css, re.DOTALL)
    assert topbar_match, ".topbar block not found in style.css"
    topbar_block = topbar_match.group(1)
    assert "rgba(255, 255, 255" not in topbar_block, (
        ".topbar has white background - will look broken on dark-mode app"
    )


def test_templates_do_not_contain_hardcoded_boosty_url() -> None:
    template_dir = Path("mailbot_v26/web_observability/templates")
    hardcoded_boosty = "https://boosty.to/personalbot/donate?qr=true"
    for template_path in template_dir.glob("*.html"):
        content = template_path.read_text(encoding="utf-8")
        assert hardcoded_boosty not in content, (
            f"Template {template_path.name} still contains hardcoded Boosty URL"
        )


def test_nav_links_have_no_span_nbsp_wrappers() -> None:
    base_template = Path("mailbot_v26/web_observability/templates/base.html")
    content = base_template.read_text(encoding="utf-8")
    assert "<span>Commitments&nbsp;</span>" not in content
    assert "<span>Latency&nbsp;</span>" not in content
    assert "<span>Attention&nbsp;</span>" not in content
    assert "<span>Learning&nbsp;</span>" not in content
    assert "<span>Relationships&nbsp;</span>" not in content
    assert "&nbsp;" not in content
    assert ">Commitments</a>" in content
    assert ">Latency</a>" in content
    assert ">Attention</a>" in content
    assert ">Learning</a>" in content
    assert ">Relationships</a>" in content
