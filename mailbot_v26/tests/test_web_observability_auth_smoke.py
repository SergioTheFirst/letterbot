from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app


FORBIDDEN = [
    "no " + "data",
    "nothing " + "to show",
    "all " + "quiet",
    "нет " + "данных",
]


def test_latency_auth_flow_and_copy(tmp_path: Path) -> None:
    db_path = tmp_path / "observability.sqlite"
    KnowledgeDB(db_path)
    app = create_app(db_path=db_path, password="pw", secret_key="secret")

    with app.test_client() as client:
        response = client.get("/latency")
        assert response.status_code == 302
        assert "/login" in response.headers.get("Location", "")

        login_resp = client.post("/login", data={"password": "pw"})
        assert login_resp.status_code in (302, 303)

        page = client.get("/latency")
        assert page.status_code == 200
        body = page.get_data(as_text=True).lower()
        for phrase in FORBIDDEN:
            assert phrase not in body
