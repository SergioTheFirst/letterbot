from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app
from mailbot_v26.tests._web_helpers import login_with_csrf

FORBIDDEN = [
    "no " + "data",
    "nothing " + "to show",
    "all " + "quiet",
    "нет " + "данных",
]


def test_latency_page_accessible_without_login_and_copy_is_clean(tmp_path: Path) -> None:
    db_path = tmp_path / "observability.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        response = client.get("/latency")
        assert response.status_code == 200
        page = response
        assert page.status_code == 200
        body = page.get_data(as_text=True).lower()
        for phrase in FORBIDDEN:
            assert phrase not in body
