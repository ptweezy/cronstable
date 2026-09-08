"""The fleet pane rebuilds its grid only when the /fleet body changes.

Drives the demo page (``docs/demo/index.html``, the byte-for-byte mirror of
the shipped dashboard plus its fake backend) in a real Chromium via
Playwright.  The page's ``window.fetch`` is wrapped so every ``/fleet``
poll is answered with a body the test picks and counted.  ``renderFleet``
keys its dirty-check on the raw response text of the payload it holds:
two polls with the same body leave the built table in place (a JS-side
property stamped on the ``<table>`` survives them), and a body with one
changed cell replaces it.

Runs wherever Playwright and its Chromium build are both present, like
``test_web_engine_parity``; the library is a dev requirement, the browser
is a separate download CI fetches in one matrix cell, so this self-skips
everywhere else.
"""

import json
import pathlib

import pytest

playwright_api = pytest.importorskip("playwright.sync_api")

DEMO = pathlib.Path(__file__).parent.parent / "docs" / "demo" / "index.html"

# Installed AFTER load (the demo backend patches window.fetch first) and
# AFTER the first jobs poll has rendered rows.  /fleet answers with
# window.__fleetBody; window.__fleetCalls counts the polls served.
_SERVE_FLEET = """
(body) => {
  window.__fleetBody = body;
  window.__fleetCalls = 0;
  const orig = window.fetch;
  window.fetch = function (url, opts) {
    const path = String(url).split("?")[0];
    if (path === "/fleet") {
      window.__fleetCalls++;
      return Promise.resolve(new Response(window.__fleetBody, {
        status: 200, headers: { "Content-Type": "application/json" }
      }));
    }
    return orig.call(this, url, opts);
  };
}
"""

# the grid's outcome cells by job row, as lists of class strings
_CELLS = """
() => Object.fromEntries(
  [...document.querySelectorAll("#fleetPanel tbody tr")].map((tr) => [
    tr.querySelector(".jobcol").textContent,
    [...tr.querySelectorAll("td.cell")].map((td) => td.className),
  ])
)
"""

FINISHED = "2026-09-06T12:00:00+00:00"


def _fleet_body(beta_on_node1):
    """A two-node, two-job payload; ``beta_on_node1`` is that cell's outcome.

    The ``last`` shape is the daemon's per-job summary on ``/fleet``.
    """

    def cell(outcome):
        return {
            "running": False,
            "enabled": True,
            "scheduled_in": 600,
            "last": {
                "outcome": outcome,
                "finished_at": FINISHED,
                "exit_code": 0 if outcome == "success" else 1,
                "duration": 1.5,
            },
        }

    nodes = [
        {
            "node_name": "node0",
            "host": None,
            "self": True,
            "status": "self",
            "as_of": FINISHED,
            "truncated": False,
            "jobs": {"alpha": cell("success"), "beta": cell("success")},
        },
        {
            "node_name": "node1",
            "host": "node1:8443",
            "self": False,
            "status": "agreed",
            "as_of": FINISHED,
            "truncated": False,
            "jobs": {"alpha": cell("success"), "beta": cell(beta_on_node1)},
        },
    ]
    # a static distribution keeps the spread-owner signature out of the
    # dirty-check, so the body text alone decides a rebuild
    return json.dumps(
        {
            "enabled": True,
            "backend": "gossip",
            "node_name": "node0",
            "distribution": "static",
            "elect_leader": False,
            "interval": 5,
            "nodes": nodes,
        }
    )


def _wait_polls(page, n):
    """Block until at least ``n`` /fleet polls have been served."""
    page.wait_for_function(
        "(n) => window.__fleetCalls >= n", arg=n, timeout=15000
    )


def _cells(page):
    return page.evaluate(_CELLS)


def _open_fleet(p):
    try:
        browser = p.chromium.launch()
    except Exception as exc:  # no chromium provisioned
        pytest.skip("playwright chromium unavailable: {}".format(exc))
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(DEMO.resolve().as_uri())
    page.wait_for_selector("#rows tr")
    # the cluster poll reveals the button once /cluster has answered
    page.wait_for_function(
        "document.getElementById('fleetBtn').style.display === ''"
    )
    page.evaluate(_SERVE_FLEET, _fleet_body("success"))
    page.evaluate("document.getElementById('fleetBtn').click()")
    page.wait_for_selector("#fleetPanel tbody td.cell")
    return browser, page, errors


def test_fleet_grid_survives_an_unchanged_body_and_follows_a_change():
    with playwright_api.sync_playwright() as p:
        browser, page, errors = _open_fleet(p)
        assert _cells(page) == {
            "alpha": ["cell ok", "cell ok"],
            "beta": ["cell ok", "cell ok"],
        }
        page.evaluate(
            "document.querySelector('#fleetPanel table').__stamp = 'built'"
        )
        served = page.evaluate("window.__fleetCalls")

        # Two more polls with the same body.  The second poll is only
        # issued once the first has been consumed and rendered (the cluster
        # poll chains loadFleet, and a pending one is joined, not doubled),
        # so by then the first has been through the dirty-check.
        _wait_polls(page, served + 2)
        assert (
            page.evaluate(
                "document.querySelector('#fleetPanel table').__stamp"
            )
            == "built"
        ), "an unchanged /fleet body rebuilt the grid"

        # one cell changes: the table is replaced and shows the new outcome
        page.evaluate(
            "(b) => { window.__fleetBody = b; }", _fleet_body("failure")
        )
        served = page.evaluate("window.__fleetCalls")
        _wait_polls(page, served + 2)
        assert (
            page.evaluate(
                "document.querySelector('#fleetPanel table').__stamp"
            )
            is None
        ), "a changed /fleet body left the stale grid in place"
        assert _cells(page) == {
            "alpha": ["cell ok", "cell ok"],
            "beta": ["cell ok", "cell fail"],
        }
        assert page.evaluate(
            "document.querySelector('#fleetPanel .fleetbar .warn').textContent"
        ) == "1 failing"

        browser.close()
    assert errors == []

