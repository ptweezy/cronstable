import os
from pathlib import Path

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")
DEMO = Path(__file__).resolve().parents[1] / "docs" / "demo" / "index.html"


def test_pool_cancellation_and_reviewed_recovery(tmp_path):
    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch(channel=os.environ.get("CRONSTABLE_TEST_BROWSER_CHANNEL"))
        except Exception as exc:
            pytest.skip(f"browser unavailable: {exc}")
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(DEMO.as_uri())
            page.wait_for_selector("#poolBody details")
            page.locator("#poolBody summary").click()
            page.locator("[data-cancel-queue]").click()
            page.wait_for_function("document.querySelector('#poolMeta').textContent === '0 waiting'")
            page.locator('[data-dagopen="data-quality-gate"]').evaluate("el => el.click()")
            page.wait_for_selector("#dgRuns tr.dagrun .rpill.failed")
            page.locator("#dgRuns tr.dagrun").filter(has=page.locator(".rpill.failed")).first.click()
            page.wait_for_function("!document.querySelector('#dgRecoverBtn').disabled")
            page.locator("#dgRecoverBtn").click()
            page.wait_for_selector("#dgRecoveryGo")
            assert "2 tasks will run; 3 task results will be reused" in page.locator("#dgRecovery").inner_text()
            page.locator("#dgRecovery summary").click()
            assert "check-volume" in page.locator("#dgRecovery").inner_text()
            page.screenshot(path=str(tmp_path / "recovery-preview.png"), full_page=True)
            page.locator("#dagDrawer").screenshot(path=str(tmp_path / "recovery-drawer.png"))
            page.locator("#dgRecoveryGo").click()
            page.wait_for_function("document.querySelector('#dgMeta').textContent.includes('recovery-')")
            page.wait_for_function("document.querySelector('#dgTasks').textContent.includes('Reused')")
            assert not errors, errors
            assert not page.locator("#dgRecovery").is_visible()
        finally:
            browser.close()
