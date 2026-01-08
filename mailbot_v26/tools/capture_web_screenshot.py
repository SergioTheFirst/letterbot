from __future__ import annotations

import importlib.util
import os
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

from mailbot_v26.config_loader import CONFIG_DIR, load_storage_config
from mailbot_v26.web_observability.app import _load_credentials, create_app


def _resolve_target_path() -> str:
    raw_url = os.environ.get("WEB_OBSERVABILITY_URL", "").strip()
    if raw_url:
        parsed = urlparse(raw_url)
        if parsed.path:
            return parsed.path
    return os.environ.get("WEB_OBSERVABILITY_PATH", "/health")


def _wait_for_login_ready(url: str, timeout_s: float = 10.0) -> bool:
    import requests

    deadline = time.monotonic() + max(0.0, timeout_s)
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.2)
    return False


def main() -> int:
    if importlib.util.find_spec("playwright") is None:
        print("Playwright not installed; screenshot capture skipped.")
        return 0

    from playwright.sync_api import sync_playwright
    from socketserver import ThreadingMixIn
    from wsgiref.simple_server import WSGIServer, make_server

    config_dir = Path(os.environ.get("WEB_OBSERVABILITY_CONFIG", "") or CONFIG_DIR)
    storage = load_storage_config(config_dir)
    db_path = Path(os.environ.get("WEB_OBSERVABILITY_DB_PATH", "") or storage.db_path)
    password, secret_key, attention_cost = _load_credentials(config_dir)

    output_path = os.environ.get("WEB_SCREENSHOT_PATH", "artifacts/health.png")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    app = create_app(
        db_path=db_path,
        password=password,
        secret_key=secret_key,
        attention_cost_per_hour=attention_cost,
    )

    class _ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
        daemon_threads = True

    server = make_server("127.0.0.1", 0, app, server_class=_ThreadedWSGIServer)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    port = server.server_port
    target_path = _resolve_target_path()
    url = f"http://127.0.0.1:{port}{target_path}"
    login_url = f"http://127.0.0.1:{port}/login"

    try:
        if not _wait_for_login_ready(login_url):
            print("Login endpoint did not become ready; screenshot capture skipped.")
            return 0
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.get_by_role("textbox", name="Password").fill(password)
            except Exception:
                page.get_by_label("Password").fill(password)
            page.get_by_role("button", name="Sign in").click()
            page.wait_for_timeout(1500)
            page.screenshot(path=str(output), full_page=True)
            browser.close()
    finally:
        server.shutdown()
        server_thread.join(timeout=5)
    print(f"Screenshot saved to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
