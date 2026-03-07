from __future__ import annotations

import re
from typing import Any


_CSRF_INPUT_RE = re.compile(r'name="csrf_token"\s+value="([^"]+)"')


def extract_csrf_token(html: str) -> str:
    match = _CSRF_INPUT_RE.search(html)
    if match is None:
        snippet = html[:200]
        raise AssertionError(f"CSRF token not found in HTML: {snippet!r}")
    return match.group(1)


def login_with_csrf(client: Any, password: str) -> None:
    login_page = client.get("/login")
    token = extract_csrf_token(login_page.get_data(as_text=True))
    response = client.post("/login", data={"password": password, "csrf_token": token})
    assert response.status_code in {200, 302, 303}


def post_doctor_export_with_csrf(client: Any):
    doctor_page = client.get("/doctor")
    token = extract_csrf_token(doctor_page.get_data(as_text=True))
    return client.post("/doctor/export", data={"csrf_token": token})
