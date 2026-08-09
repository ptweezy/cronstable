"""A job row's Logs button opens the drawer on a live logs tab.

Drives the demo page (``docs/demo/index.html``, the byte-for-byte mirror of
the shipped dashboard plus its fake backend) in a real Chromium via
Playwright.  The row actions dispatch through one delegated handler that
calls every action as ``fn(name, btn)``, and ``openDrawer``'s second
parameter is a tab NAME: forwarding the button element there once left the
drawer with no active tab, no visible pane and no log stream.  This pins
the wrapped registration: the drawer must open, the logs pane must be the
active one, and the tail fetch for the clicked job must go out.

Runs wherever Playwright and its Chromium build are both present, like
``test_web_engine_parity``; the library is a dev requirement, the browser
is a separate download CI fetches in one matrix cell, so this self-skips
everywhere else.
"""

import pathlib

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")

DEMO = pathlib.Path(__file__).parent.parent / "docs" / "demo" / "index.html"

# installed AFTER load (the demo backend patches window.fetch first, so the
# wrapper sees exactly what the app requests) and AFTER the first jobs poll
# has rendered rows, so nothing recorded predates the button click
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


def test_row_logs_button_opens_the_logs_tab():
    with playwright_api.sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception as exc:  # no chromium provisioned
            pytest.skip("playwright chromium unavailable: {}".format(exc))
        page = browser.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(DEMO.resolve().as_uri())
        page.wait_for_selector("#rows tr")
        page.evaluate(_RECORD_FETCH)
        name = page.evaluate(
            "document.querySelector('#rows [data-logs]')"
            ".getAttribute('data-logs')"
        )
        page.evaluate("document.querySelector('#rows [data-logs]').click()")
        page.wait_for_selector('#drawer[aria-hidden="false"]')
        # the discriminating assertions: a bogus tab argument strips .active
        # from every tab button and pane, and stops the stream instead of
        # starting it
        pane_active = page.evaluate(
            "document.querySelector('.pane[data-pane=\"logs\"]')"
            ".classList.contains('active')"
        )
        tab_active = page.evaluate(
            "(document.querySelector('#dTabs button.active') || {})"
            ".getAttribute && document.querySelector('#dTabs button.active')"
            ".getAttribute('data-tab')"
        )
        # the demo backend serves the tail as a real SSE response, so the
        # connect shows up in the recorded paths almost immediately
        page.wait_for_function(
            "window.__paths.some((x) => x.endsWith('/logs'))"
        )
        paths = page.evaluate("window.__paths")
        browser.close()
    assert not errors, errors
    assert pane_active, "logs pane lost .active: openDrawer got a bad tab"
    assert tab_active == "logs"
    assert "/jobs/" + name + "/logs" in paths
