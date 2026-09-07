"""The log drawer's search counts, hides, and re-parses streamed lines.

Drives the demo page (``docs/demo/index.html``, the byte-for-byte mirror of
the shipped dashboard plus its fake backend) in a real Chromium via
Playwright.  The page's ``window.fetch`` is wrapped so the drawer's tail
request gets a stream the test feeds, line by line, through the same SSE
reader and ``appendLine`` path a daemon's output takes.  Three caches sit
on that path and this pins each one through the count label and the
row visibility:

* the compiled regex is memoized by its source, with a "bad" state for a
  pattern that does not compile, so "(" reads "bad regex" and hides no row,
  and a repaired pattern counts again;
* each buffered line caches its ANSI-stripped and lowercased text, so a
  plain query matches an uppercase line and an anchored regex matches a
  line whose color code precedes the word;
* a line appended while a query is active adjusts the count by one
  without a rescan of the buffer.

Runs wherever Playwright and its Chromium build are both present, like
``test_web_engine_parity``; the library is a dev requirement, the browser
is a separate download CI fetches in one matrix cell, so this self-skips
everywhere else.
"""

import pathlib

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")

DEMO = pathlib.Path(__file__).parent.parent / "docs" / "demo" / "index.html"

# Installed AFTER load (the demo backend patches window.fetch first) and
# AFTER the first jobs poll has rendered rows.  Every job tail is answered
# with one open-ended SSE stream whose controller is parked on window, so
# the test pushes frames through the page's own reader.  The stream never
# ends: the drawer keeps it as the live stream and does not re-attach the
# demo's scripted tail on the next poll.
_SERVE_TAIL = """
() => {
  window.__tail = null;
  const orig = window.fetch;
  window.fetch = function (url, opts) {
    const path = String(url).split("?")[0];
    if (/^\\/jobs\\/[^/]+\\/logs$/.test(path)) {
      const body = new ReadableStream({
        start(ctrl) { window.__tail = ctrl; },
      });
      return Promise.resolve(new Response(body, {
        status: 200, headers: { "Content-Type": "text/event-stream" }
      }));
    }
    return orig.call(this, url, opts);
  };
}
"""

# one SSE "line" frame per call, in the daemon's wire shape
_PUSH_LINE = """
(text) => {
  const frame = "event: line\\ndata: " +
    JSON.stringify({ stream: "stdout", line: text }) + "\\n\\n";
  window.__tail.enqueue(new TextEncoder().encode(frame));
}
"""

# gutter number -> whether the row is displayed
_VISIBLE_ROWS = """
() => Object.fromEntries(
  [...document.querySelectorAll("#term .ln")].map((el) => [
    el.querySelector(".gut").textContent, el.style.display !== "none",
  ])
)
"""

# Six lines: three carry "needle" at the start of their visible text, in
# plain, color-coded, and uppercase form; three do not.
LINES = [
    "needle one",
    "hay only",
    "\x1b[31mneedle\x1b[0m in red",
    "more hay",
    "NEEDLE shouted",
    "hay again",
]
MATCHING = {"1", "3", "5"}
NOT_MATCHING = {"2", "4", "6"}


def _wait_count(page, expected):
    """Wait for the label to read ``expected``, then assert it exactly."""
    try:
        page.wait_for_function(
            "(want) => document.getElementById('logCount')"
            ".textContent === want",
            arg=expected,
            timeout=5000,
        )
    except playwright_api.TimeoutError:
        pass
    label = _label(page)
    assert label == expected, label


def _label(page):
    return page.evaluate("document.getElementById('logCount').textContent")


def _visible(page):
    return page.evaluate(_VISIBLE_ROWS)


def _open_drawer_with_lines(p):
    try:
        browser = p.chromium.launch()
    except Exception as exc:  # no chromium provisioned
        pytest.skip("playwright chromium unavailable: {}".format(exc))
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(DEMO.resolve().as_uri())
    page.wait_for_selector("#rows tr")
    page.evaluate(_SERVE_TAIL)
    page.evaluate("document.querySelector('#rows [data-logs]').click()")
    page.wait_for_selector('#drawer[aria-hidden="false"]')
    page.wait_for_function("window.__tail !== null")
    for text in LINES:
        page.evaluate(_PUSH_LINE, text)
    page.wait_for_function(
        "(n) => document.querySelectorAll('#term .ln').length === n",
        arg=len(LINES),
    )
    return browser, page, errors


def test_log_search_counts_hides_and_recompiles():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_drawer_with_lines(p)
        assert _label(page) == ""

        # plain query: the uppercase line matches through the lowercased
        # cache; every row stays displayed until "matches only" is on
        page.fill("#logSearch", "needle")
        _wait_count(page, "3 matches")
        vis = _visible(page)
        assert set(vis) == MATCHING | NOT_MATCHING
        assert all(vis.values()), vis

        # "matches only" is a synchronous rerender on change
        page.check("#optOnly")
        _wait_count(page, "3 matches")
        vis = _visible(page)
        assert {k for k, v in vis.items() if v} == MATCHING, vis
        assert {k for k, v in vis.items() if not v} == NOT_MATCHING, vis

        # regex mode on the same source compiles it as a pattern
        page.check("#optRegex")
        _wait_count(page, "3 matches")
        assert {k for k, v in _visible(page).items() if v} == MATCHING

        # a pattern that does not compile: the label says so and no row is
        # hidden
        page.fill("#logSearch", "(")
        _wait_count(page, "bad regex")
        assert all(_visible(page).values())

        # the anchored pattern matches the color-coded line only through
        # its ANSI-stripped cache, and the "i" flag covers the uppercase one
        page.fill("#logSearch", "^nee+dle")
        _wait_count(page, "3 matches")
        vis = _visible(page)
        assert {k for k, v in vis.items() if v} == MATCHING, vis
        assert {k for k, v in vis.items() if not v} == NOT_MATCHING, vis

        # one streamed line while the query is active: the count moves by
        # one and the new row is displayed under "matches only"
        page.evaluate(_PUSH_LINE, "needle seven")
        _wait_count(page, "4 matches")
        vis = _visible(page)
        assert vis["7"] is True, vis
        assert {k for k, v in vis.items() if v} == MATCHING | {"7"}, vis

        # and a non-matching one leaves the count alone and stays hidden
        page.evaluate(_PUSH_LINE, "trailing hay")
        page.wait_for_function(
            "document.querySelectorAll('#term .ln').length === 8"
        )
        _wait_count(page, "4 matches")
        assert _visible(page)["8"] is False

        browser.close()
    assert errors == []


def test_bad_pattern_reads_bad_regex():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_drawer_with_lines(p)
        page.check("#optRegex")
        page.fill("#logSearch", "(")
        _wait_count(page, "bad regex")
        browser.close()
    assert errors == []
