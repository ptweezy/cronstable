"""The tab title reports live fleet state in plain words.

Drives the demo page (``docs/demo/index.html``, the byte-for-byte mirror of
the shipped dashboard plus its fake backend) in a real Chromium via
Playwright.  After the first poll the static ``<title>`` gives way to the
worst-condition ladder (no signal / cluster alert / failing / running /
quiet with the next fire), worded outright with middle-dot separators and
no status glyphs.  A state with more than one line to show rotates
complete readouts on a fixed dwell.  The favicon is the static cronstable
mark, a ``data:`` URI inlined in the head so the page remains one file.

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

# a state readout, then the brand the demo backend installs
LADDER = re.compile(r"^\S.* · cronstable demo$")

# the live title is in place once the middle-dot brand suffix appears (the
# boot ``<title>`` carries no middle dot before the brand)
_TITLE_LIVE = "() => / \\u00b7 cronstable demo$/.test(document.title)"

# quiet-state frames after a recovery: the healthy count or the next fire
QUIET = re.compile(r"^(\d+(?:/\d+)? ok|\d+ jobs|next: .+) · cronstable demo$")

# Drive the state directly through the ?perf=1 hook, then read the title in
# the same evaluation so a real 3s poll cannot overwrite the forced fleet
# between the mutation and the assertion.  tick() runs the title refresh.
_FORCE_FAILURE = """
() => {
  const s = window.__perf.state();
  s.clusterAlert = null;
  s.jobs.forEach((j) => {
    j.running = false;
    j.enabled = true;
    j.paused = null;
    j.last_run = { outcome: "success", exit_code: 0,
                   finished_at: new Date().toISOString() };
  });
  const j = s.jobs[0];
  j.last_run = { outcome: "failure", exit_code: 1,
                 finished_at: new Date().toISOString() };
  window.__perf.tick();
  return { name: j.name, title: document.title };
}
"""

# Force two failing jobs and sample the title twice, one flip dwell apart.
# The frame index derives from the wall clock, so the samples land on
# different frames of the three-frame rotation (count, then each name);
# re-forcing before the second sample keeps a real demo poll from
# rewriting the fleet in between.
_SAMPLE_FLIP = """
async () => {
  const force = () => {
    const s = window.__perf.state();
    s.clusterAlert = null;
    s.jobs.forEach((j, i) => {
      j.running = false;
      j.enabled = true;
      j.paused = null;
      j.last_run = { outcome: i < 2 ? "failure" : "success",
                     exit_code: i < 2 ? 1 : 0,
                     finished_at: new Date().toISOString() };
    });
    window.__perf.tick();
    return document.title;
  };
  const first = force();
  await new Promise((resolve) => setTimeout(resolve, 4100));
  const second = force();
  const s = window.__perf.state();
  return { names: [s.jobs[0].name, s.jobs[1].name],
           first: first, second: second };
}
"""

_FORCE_RECOVERY = """
() => {
  const s = window.__perf.state();
  s.jobs.forEach((j) => {
    j.running = false;
    j.last_run = { outcome: "success", exit_code: 0,
                   finished_at: new Date().toISOString() };
  });
  window.__perf.tick();
  return { title: document.title };
}
"""


def _open_demo(p, query=""):
    try:
        browser = p.chromium.launch()
    except Exception as exc:  # no chromium provisioned
        pytest.skip("playwright chromium unavailable: {}".format(exc))
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(DEMO.resolve().as_uri() + query)
    # the title refresh rides the poll pipeline, so the first poll must land
    page.wait_for_selector("#rows tr")
    page.wait_for_function(_TITLE_LIVE)
    return browser, page, errors


def test_demo_title_walks_the_ladder():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_demo(p)
        title = page.evaluate("document.title")
        assert LADDER.match(title), title
        icon = page.evaluate(
            "document.querySelector('link[rel=icon]').href"
        )
        assert icon.startswith("data:image/png")
        assert errors == []
        browser.close()


def test_demo_title_escalates_on_failure_and_recovers():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_demo(p, "?perf=1")
        failed = page.evaluate(_FORCE_FAILURE)
        assert failed["title"] == "{} failing · cronstable demo".format(
            failed["name"]
        )
        recovered = page.evaluate(_FORCE_RECOVERY)
        assert QUIET.match(recovered["title"]), recovered["title"]
        assert errors == []
        browser.close()


def test_demo_title_rotates_complete_readouts():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_demo(p, "?perf=1")
        r = page.evaluate(_SAMPLE_FLIP)
        suffix = " · cronstable demo"
        expected = {"2 jobs failing" + suffix}
        expected |= {"{} failing{}".format(n, suffix) for n in r["names"]}
        assert r["first"] in expected, r["first"]
        assert r["second"] in expected, r["second"]
        assert r["first"] != r["second"]
        assert errors == []
        browser.close()
