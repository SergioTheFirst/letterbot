"""QW15 (finding C2): relationships pages must not reflect payloads unescaped.

The graph/contact JSON sits in single-quoted data-* attributes. json.dumps
does not escape a single quote, so a value containing ' used to close the
attribute and inject event handlers.
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

from mailbot_v26.storage.knowledge_db import KnowledgeDB
from mailbot_v26.web_observability.app import create_app

ATTR_PAYLOAD = "x'onmouseover='alert(1)"
TAG_PAYLOAD = "</div><script>alert(2)</script>"


def _app(tmp_path: Path):
    db_path = tmp_path / "rel.sqlite"
    KnowledgeDB(db_path)
    return create_app(db_path=db_path, password="pw", secret_key="secret")


def _data_attr(body: str, name: str) -> object:
    match = re.search(rf"data-{name}='([^']*)'", body)
    assert match is not None, f"data-{name} attribute missing"
    return json.loads(html.unescape(match.group(1)))


def test_contact_page_escapes_contact_id_from_query(tmp_path: Path) -> None:
    app = _app(tmp_path)
    with app.test_client() as client:
        response = client.get(
            "/relationships/contact",
            query_string={"account_email": "acct@example.com", "contact_id": ATTR_PAYLOAD},
        )
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert ATTR_PAYLOAD not in body
    assert "onmouseover='" not in body
    detail = _data_attr(body, "contact")
    assert isinstance(detail, dict)
    assert detail["contact"]["id"] == f"contact:{ATTR_PAYLOAD}"


def test_graph_page_escapes_node_strings(tmp_path: Path) -> None:
    app = _app(tmp_path)
    graph = {
        "nodes": [
            {"id": "user:me", "label": "me"},
            {"id": "contact:a", "display_name": ATTR_PAYLOAD, "label": TAG_PAYLOAD},
        ],
        "edges": [],
    }

    class FakeAnalytics:
        def relationship_graph(self, **kwargs):
            return graph

    app.config["ANALYTICS_FACTORY"] = lambda: FakeAnalytics()
    with app.test_client() as client:
        response = client.get(
            "/relationships", query_string={"account_email": "acct@example.com"}
        )
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert ATTR_PAYLOAD not in body
    assert TAG_PAYLOAD not in body
    assert "<script>alert(2)" not in body
    assert _data_attr(body, "graph") == graph
