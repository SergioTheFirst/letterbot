"""QW16 (findings C5/C6/C7): bounded web cache, no discarded work on polls."""

from __future__ import annotations

import re
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.tests.test_web_cockpit_home import FakeAnalytics
from mailbot_v26.web_observability import app as web_app
from mailbot_v26.web_observability.app import create_app

TEMPLATES = Path(web_app.__file__).parent / "templates"


def test_ttl_cache_stays_bounded_under_many_keys() -> None:
    cache = web_app._TTLCache(600.0)
    for index in range(5_000):
        cache.set(("key", index), index)
    assert len(cache._items) <= 256
    # Oldest entries are evicted first; the newest stay readable.
    assert cache.get(("key", 4_999)) == 4_999
    assert cache.get(("key", 0)) is None


def test_ttl_cache_prunes_expired_entries_on_set(monkeypatch) -> None:
    clock = {"now": 1_000.0}
    monkeypatch.setattr(web_app.time, "monotonic", lambda: clock["now"])
    cache = web_app._TTLCache(10.0)
    for index in range(50):
        cache.set(("bucket", index), index)
    clock["now"] += 11.0
    cache.set(("bucket", "fresh"), "fresh")
    assert list(cache._items) == [("bucket", "fresh")]


class _RecordingAnalytics(FakeAnalytics):
    def __init__(self) -> None:
        super().__init__()
        self.unused_calls: list[str] = []

    def cockpit_top_senders(self, *args, **kwargs):
        self.unused_calls.append("cockpit_top_senders")
        return []

    def cockpit_silent_contacts(self, *args, **kwargs):
        self.unused_calls.append("cockpit_silent_contacts")
        return []

    def cockpit_stalled_threads(self, *args, **kwargs):
        self.unused_calls.append("cockpit_stalled_threads")
        return []


def _app(tmp_path: Path):
    db_path = tmp_path / "polls.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_index_does_not_query_unrendered_contact_lists(tmp_path: Path) -> None:
    app = _app(tmp_path)
    analytics = _RecordingAnalytics()
    app.config["ANALYTICS_FACTORY"] = lambda: analytics
    with app.test_client() as client:
        response = client.get("/?account_emails=primary@example.com")
    assert response.status_code == 200
    assert analytics.unused_calls == []


class _RecordingCache:
    def __init__(self) -> None:
        self.lookups: list[tuple[object, ...]] = []

    def get(self, key):
        self.lookups.append(key)
        return None

    def set(self, key, value) -> None:
        return None


def test_health_partial_skips_discarded_summary(tmp_path: Path, monkeypatch) -> None:
    recorder = _RecordingCache()
    monkeypatch.setattr(web_app, "_HEALTH_SUMMARY_CACHE", recorder)
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get("/partial/health?account_emails=primary@example.com")
    assert response.status_code == 200
    # Only the component matrix (_health_status_payload) needs the summary; the
    # route's own summary call was computed and thrown away.
    assert len(recorder.lookups) == 1


def test_dashboard_cache_outlives_poll_interval(tmp_path: Path) -> None:
    assert web_app._DASHBOARD_CACHE_TTL * 1000 >= web_app.DASHBOARD_REFRESH_MS
    app = _app(tmp_path)
    app.config["ANALYTICS_FACTORY"] = lambda: FakeAnalytics()
    with app.test_client() as client:
        body = client.get("/").get_data(as_text=True)
    assert f"refreshDashboard(); }}, {web_app.DASHBOARD_REFRESH_MS});" in body


def test_polling_intervals_pause_while_page_hidden() -> None:
    for name in ("cockpit.html", "health.html"):
        source = (TEMPLATES / name).read_text(encoding="utf-8")
        intervals = re.findall(r"setInterval\([^;]*\);", source)
        assert intervals, name
        for call in intervals:
            assert "document.hidden" in call, f"{name}: {call}"
        # A visibility return still triggers an immediate refresh.
        assert "visibilitychange" in source, name
