"""A job row's Logs button opens the drawer on a live logs tab, and an open
History tab follows the job's runs.

The first test drives the demo page (``docs/demo/index.html``, the
byte-for-byte mirror of the shipped dashboard plus its fake backend) in a
real Chromium via Playwright.  The row actions dispatch through one
delegated handler that calls every action as ``fn(name, btn)``, and
``openDrawer``'s second parameter is a tab NAME: forwarding the button
element there once left the drawer with no active tab, no visible pane and
no log stream.  This pins the wrapped registration: the drawer must open,
the logs pane must be the active one, and the tail fetch for the clicked
job must go out.  The History tests run against a real daemon.

Runs wherever Playwright and its Chromium build are both present, like
``test_web_engine_parity``; the library is a dev requirement, the browser
is a separate download CI fetches in one matrix cell, so this self-skips
everywhere else.
"""

import pathlib

import pytest

pytest.importorskip("playwright.sync_api")

from tests import _web_e2e as e2e  # noqa: E402

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


@pytest.fixture(scope="module")
def browser():
    with e2e.browser_session() as b:
        yield b


def test_row_logs_button_opens_the_logs_tab(browser):
    page = browser.new_page()
    try:
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
    finally:
        page.close()
    assert not errors, errors
    assert pane_active, "logs pane lost .active: openDrawer got a bad tab"
    assert tab_active == "logs"
    assert "/jobs/" + name + "/logs" in paths


# Records whether the History pane ever shows its loading placeholder.
_WATCH_PLACEHOLDER = """
() => {
  const pane = document.getElementById("historyPane");
  window.__placeholder = false;
  new MutationObserver(() => {
    if (pane.textContent.includes("loading run history"))
      window.__placeholder = true;
  }).observe(pane, { childList: true, subtree: true });
}
"""


def _open_history(page, name):
    page.evaluate(
        "(n) => document.querySelector("
        "'#rows [data-logs=\"' + n + '\"]').click()",
        name,
    )
    page.wait_for_selector('#drawer[aria-hidden="false"]')
    page.evaluate(
        "document.querySelector('#dTabs button[data-tab=\"history\"]').click()"
    )


def _history_rows(page):
    return page.evaluate(
        "[...document.querySelectorAll('#historyPane .runtable tbody tr')]"
        ".map((tr) => [...tr.cells].map((td) => td.textContent.trim()))"
    )


def test_open_history_tab_adds_a_new_run_in_place(browser, tmp_path):
    with e2e.Daemon(tmp_path) as daemon:
        for _ in range(6):
            daemon.run_and_wait("alpha-ok")
        with e2e.open_page(
            browser,
            daemon.url,
            prefs={"pollMs": 1000},
            viewport={"width": 1440, "height": 460},
        ) as page:
            page.faults.record()
            _open_history(page, "alpha-ok")
            page.wait_for_selector("#historyPane .runtable")
            assert len(_history_rows(page)) == 6
            page.evaluate(_WATCH_PLACEHOLDER)
            page.evaluate(
                "document.querySelector('#historyPane .runtable')"
                ".dataset.mark = 'kept'"
            )
            # polls that bring no new run leave the pane as it is
            polls = len(page.faults.sent("GET", "/jobs"))
            e2e.wait_until(
                lambda: len(page.faults.sent("GET", "/jobs")) >= polls + 2,
                page,
            )
            assert len(page.faults.sent("GET", "/jobs/alpha-ok/runs")) == 1
            assert page.query_selector("#historyPane .runtable[data-mark]")
            pane = "document.getElementById('historyPane')"
            assert page.evaluate(
                pane + ".scrollHeight > " + pane + ".clientHeight"
            )
            top = page.evaluate(
                "((p) => { p.scrollTop = 40; return p.scrollTop; })("
                + pane
                + ")"
            )
            assert top > 0
            # a new run appears on the next poll, in the same scroll place
            daemon.run_and_wait("alpha-ok")
            page.wait_for_function(
                "document.querySelectorAll("
                "'#historyPane .runtable tbody tr').length === 7",
                timeout=3000,
            )
            assert page.evaluate(pane + ".scrollTop") == top
            assert page.evaluate("window.__placeholder") is False


def test_open_history_tab_lists_pause_skipped_slots(browser, tmp_path):
    jobs = [e2e.job("ticker", "echo tick", schedule="* * * * * * *")]
    with e2e.Daemon(tmp_path, jobs=jobs) as daemon:
        status, body = daemon.api("POST", "/jobs/ticker/pause")
        assert status == 200, body
        with e2e.open_page(
            browser, daemon.url, prefs={"pollMs": 1000}
        ) as page:
            _open_history(page, "ticker")
            page.wait_for_selector(
                "#historyPane .term-empty, #historyPane .runtable"
            )
            count = len(_history_rows(page))
            # the pause holds back a slot every second, and each one shows
            # up in the open tab with its reason
            page.wait_for_function(
                "(n) => document.querySelectorAll("
                "'#historyPane .runtable tbody tr').length >= n",
                arg=count + 2,
            )
            skipped = [r for r in _history_rows(page) if r[0] == "⊘ skipped"]
            assert len(skipped) >= 2
            assert {r[-1] for r in skipped} == {"paused"}
