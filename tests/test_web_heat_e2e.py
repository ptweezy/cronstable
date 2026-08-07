"""The dashboard's activity heatmap fills from the batched /activity feed.

Drives the demo page (``docs/demo/index.html``, the byte-for-byte mirror of
the shipped dashboard plus its fake backend) in a real Chromium via
Playwright, with the page's own ``window.fetch`` wrapped to record every
request path.  Two ends of the contract:

* against a daemon that serves ``GET /activity`` (the demo backend does),
  opening the heat panel issues exactly one batched fetch and not one
  ``GET /jobs/{name}/runs`` per job;
* against a daemon that predates the endpoint (the wrapper answers the
  probe with a 404), the panel still renders, filled by the per-job
  fallback loop.

Runs wherever Playwright and its Chromium build are both present, like
``test_web_engine_parity``; the library is a dev requirement, the browser
is a separate download CI fetches in one matrix cell, so this self-skips
everywhere else.
"""

import pathlib
import re

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")

DEMO = pathlib.Path(__file__).parent.parent / "docs" / "demo" / "index.html"

# installed AFTER load (the demo backend patches window.fetch first, so the
# wrapper sees exactly what the app requests) and AFTER the first jobs poll
# has rendered rows, so nothing recorded predates the heat click
_RECORD_FETCH = """
() => {
  window.__paths = [];
  const orig = window.fetch;
  window.fetch = function (url, opts) {
    window.__paths.push(String(url).split("?")[0]);
    return orig.call(this, url, opts);
  };
}
"""

# same recorder, but the /activity probe is answered the way an unmatched
# aiohttp path answers: a 404 with the daemon's JSON error envelope
_RECORD_FETCH_NO_ACTIVITY = """
() => {
  window.__paths = [];
  const orig = window.fetch;
  window.fetch = function (url, opts) {
    const path = String(url).split("?")[0];
    window.__paths.push(path);
    if (path === "/activity") {
      return Promise.resolve(new Response('{"error": "not found"}', {
        status: 404, headers: { "Content-Type": "application/json" }
      }));
    }
    return orig.call(this, url, opts);
  };
}
"""


def _open_demo(p):
    try:
        browser = p.chromium.launch()
    except Exception as exc:  # no chromium provisioned
        pytest.skip("playwright chromium unavailable: {}".format(exc))
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(DEMO.resolve().as_uri())
    # the heat render iterates the jobs snapshot, so the first poll must
    # have landed before the panel opens
    page.wait_for_selector("#rows tr")
    return browser, page, errors


def test_demo_heatmap_fills_from_one_activity_fetch():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_demo(p)
        page.evaluate(_RECORD_FETCH)
        page.evaluate("document.getElementById('heatBtn').click()")
        page.wait_for_selector(".heat-cell.act")
        paths = page.evaluate("window.__paths")
        browser.close()
    assert not errors, errors
    assert paths.count("/activity") == 1
    runs = [x for x in paths if re.match(r"^/jobs/.+/runs$", x)]
    assert runs == [], (
        "the batched feed was served, yet the page still fanned out "
        "per job: {}".format(runs)
    )


def test_demo_heatmap_falls_back_per_job_without_activity():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_demo(p)
        page.evaluate(_RECORD_FETCH_NO_ACTIVITY)
        page.evaluate("document.getElementById('heatBtn').click()")
        page.wait_for_selector(".heat-cell.act")
        paths = page.evaluate("window.__paths")
        browser.close()
    assert not errors, errors
    assert paths.count("/activity") == 1  # the probe, answered 404
    runs = [x for x in paths if re.match(r"^/jobs/.+/runs$", x)]
    assert runs, (
        "no per-job fallback fetches: a new page against an old daemon "
        "would render an empty punchcard"
    )
