from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def main() -> int:
    if importlib.util.find_spec("playwright") is None:
        print("Playwright not installed; screenshot capture skipped.")
        return 2

    from playwright.sync_api import sync_playwright

    url = os.environ.get("WEB_OBSERVABILITY_URL", "http://127.0.0.1:8080/health")
    password = os.environ.get("WEB_PASSWORD", "")
    output_path = os.environ.get("WEB_SCREENSHOT_PATH", "artifacts/health.png")
    if not password:
        print("WEB_PASSWORD is required for screenshot capture.")
        return 2

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded")
        password_input = page.get_by_label("Password")
        password_input.fill(password)
        page.get_by_role("button", name="Sign in").click()
        page.wait_for_timeout(1500)
        page.screenshot(path=str(output), full_page=True)
        browser.close()
    print(f"Screenshot saved to {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
