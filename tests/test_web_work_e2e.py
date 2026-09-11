import os
from pathlib import Path

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")
DEMO = Path(__file__).resolve().parents[1] / "docs" / "demo" / "index.html"


def test_queue_states_and_verification_only_logs(tmp_path):
    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                channel=os.environ.get("CRONSTABLE_TEST_BROWSER_CHANNEL")
            )
        except Exception as exc:
            pytest.skip(f"browser unavailable: {exc}")
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(DEMO.as_uri())
            page.wait_for_selector("#rows tr[data-job]")
            page.evaluate("""async () => {
              const fetch = window.fetch;
              const base = (await (await fetch('/jobs')).json())[0];
              const now = Date.now() / 1000;
              const entries = [
                {id:'low', job:'queued-export', state:'queued', slots:1,
                 priority:0, queuedAt:now-60, expiresAt:now+600},
                {id:'high', job:'queued-export', state:'queued', slots:1,
                 priority:10, queuedAt:now-30, expiresAt:now+600}
              ];
              const jobs = [
                {...base, name:'checked-export', enabled:true, running:true,
                 paused:null, captureStdout:false, captureStderr:false,
                 verification:{configured:true,running:true}},
                {...base, name:'queued-export', enabled:true, running:false,
                 paused:null,
                 pool:{name:'warehouse',slots:1,priority:0,queued:entries}}
              ];
              window.poolUnavailable = false;
              window.fetch = (input, init) => {
                const path = String(input).split('?')[0];
                if (path === '/jobs') {
                  return Promise.resolve(Response.json(jobs));
                }
                if (path === '/pools') return Promise.resolve(
                  window.poolUnavailable
                  ? Response.json({error:'Unavailable'}, {status:503})
                  : Response.json([{name:'warehouse',slots:2,
                     configuredSlots:1,used:2,queued:2,entries}]));
                if (path === '/jobs/checked-export/logs') {
                  const stream = new ReadableStream({start(controller) {
                    controller.enqueue(new TextEncoder().encode(
                      'event: line\\ndata: {"stream":"verify.stderr",' +
                      '"line":"missing output"}\\n\\n'));
                  }});
                  return Promise.resolve(new Response(stream, {
                    headers:{'Content-Type':'text/event-stream'}}));
                }
                return fetch(input, init);
              };
            }""")
            page.locator("#refreshBtn").click()
            page.wait_for_selector(
                '#rows [data-job="checked-export"] .st.verifying'
            )
            queued = page.locator('#rows [data-job="queued-export"]')
            assert queued.locator(".st.queued").inner_text().endswith("Queued")
            assert queued.locator("[data-run]").inner_text() == "Queue another"
            queued.locator("[data-open-pool]").click()
            page.wait_for_selector("#poolBody details[open]")
            assert "Draining" in page.locator("#poolBody").inner_text()
            assert (
                page.locator("[data-cancel-queue]").first.get_attribute(
                    "data-cancel-queue"
                )
                == "high"
            )
            page.screenshot(
                path=str(tmp_path / "pools-desktop.png"), full_page=True
            )

            page.locator('[data-logs="checked-export"]').click()
            page.wait_for_selector('#term [class~="verify.stderr"]')
            assert "[verification]" in page.locator("#term").inner_text()
            page.locator("#dClose").click()
            page.locator("#tailBtn").click()
            page.locator("#tailAddInput").fill("checked-export")
            page.locator("#tailAddInput").press("Enter")
            page.wait_for_selector('#tailTerm [class~="verify.stderr"]')
            assert "missing output" in page.locator("#tailTerm").inner_text()
            page.locator("#tailClose").click()

            page.evaluate("window.poolUnavailable = true")
            page.locator("#refreshBtn").click()
            page.wait_for_function(
                "document.querySelector('#poolMeta').textContent.includes('Unavailable')"
            )
            assert (
                "showing data from" in page.locator("#poolMeta").text_content()
            )
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate(
                "document.documentElement.scrollWidth <= innerWidth"
            )
            page.screenshot(
                path=str(tmp_path / "pools-mobile.png"), full_page=True
            )
            assert not errors, errors
        finally:
            browser.close()


def test_failed_dates_recovery_reports_batch_and_retries_same_plan(tmp_path):
    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                channel=os.environ.get("CRONSTABLE_TEST_BROWSER_CHANNEL")
            )
        except Exception as exc:
            pytest.skip(f"browser unavailable: {exc}")
        try:
            page = browser.new_page(viewport={"width": 1100, "height": 900})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(DEMO.as_uri())
            page.wait_for_selector("#rows tr[data-job]")
            page.evaluate("""() => {
              const fetch = window.fetch;
              window.recoveryRequests = [];
              window.fetch = async (input, init) => {
                if (String(input).endsWith('/data-quality-gate/recover')) {
                  const body = JSON.parse(init.body);
                  if (!body.dryRun) {
                    window.recoveryRequests.push(body);
                    if (window.recoveryRequests.length === 1) {
                      throw new TypeError('Connection lost');
                    }
                  }
                }
                return fetch(input, init);
              };
            }""")
            page.locator('[data-dagopen="data-quality-gate"]').evaluate(
                "el => el.click()"
            )
            page.wait_for_selector("#dgRuns tr.dagrun")
            page.locator("#dgBackfillBtn").click()
            page.locator("#dgBfFrom").fill("2020-01-01T00:00")
            page.locator("#dgBfTo").fill("2030-01-01T00:00")
            page.locator("#dgBfFailed").check()
            page.locator("#dgBfGo").click()
            page.wait_for_selector("#dgRecoveryGo")
            page.locator("#dgRecoveryGo").click()
            page.wait_for_selector("#dgRecoveryError")
            assert "Retry" in page.locator("#dgRecoveryGo").inner_text()
            page.locator("#dgRecoveryGo").click()
            page.wait_for_selector("#dgRecoveryResults a[data-source-run]")
            requests = page.evaluate("window.recoveryRequests")
            assert len(requests) == 2
            assert requests[0] == requests[1]
            results = page.locator("#dgRecoveryResults")
            assert "finished" in results.inner_text()
            page.locator("#dgRecoveryResults a[data-source-run]").first.click()
            page.wait_for_selector("#dgMeta a[data-source-run]")
            assert "reused" in page.locator("#dgMeta").inner_text()
            page.locator("#dagDrawer").screenshot(
                path=str(tmp_path / "recovery-batch.png")
            )
            assert not errors, errors
        finally:
            browser.close()
