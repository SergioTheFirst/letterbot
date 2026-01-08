from pathlib import Path

import pytest
import sqlite3

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


def _app(tmp_path: Path):
    db_path = tmp_path / "observability.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def test_web_analytics_connections_read_only(tmp_path: Path) -> None:
    app = _app(tmp_path)
    analytics = app.config["ANALYTICS_FACTORY"]()
    with analytics._connect_readonly() as conn:
        query_only = conn.execute("PRAGMA query_only").fetchone()
        assert query_only is not None
        assert int(query_only[0]) == 1
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE temp_guard(x INTEGER)")


def test_web_pages_avoid_empty_state_phrases(tmp_path: Path) -> None:
    app = _app(tmp_path)
    forbidden_phrases = [
        "no " + "data",
        "nothing " + "to show",
        "all " + "quiet",
        "нет " + "данных",
    ]
    with app.test_client() as client:
        client.post("/login", data={"password": "pw"})
        for path in [
            "/",
            "/latency",
            "/attention",
            "/learning",
            "/events",
            "/health",
            "/relationships",
            "/archive",
            "/email/1",
        ]:
            response = client.get(path)
            if path.startswith("/email/"):
                assert response.status_code in {200, 404}
            else:
                assert response.status_code == 200
            body = response.get_data(as_text=True).lower()
            for phrase in forbidden_phrases:
                assert phrase not in body
